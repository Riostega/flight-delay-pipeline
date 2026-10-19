"""Tests for the watchdog's decisions: when to restart Airflow, when to alert, when to stay quiet.

Nothing here touches systemd, Airflow, Slack or Snowflake. Every outside call is replaced,
so these run anywhere, CI included. They pin down the guards that keep the automatic
restart safe (once per outage, a cooldown, never around the 09:00 UTC flights run, never
on an inconclusive check) and the alert bookkeeping (throttle, changed wording, retries).
"""
import subprocess
from datetime import datetime, timedelta, timezone

import pytest

from pipeline import watchdog

NOW = datetime(2026, 10, 19, 14, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    """Every test gets its own state file, never the real one."""
    monkeypatch.setattr(watchdog, "STATE_FILE", tmp_path / "state.json")


def completed(returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


# ------------------------------------------------------------ flights window

@pytest.mark.parametrize("hhmm, inside", [
    ("08:49", False), ("08:50", True), ("09:00", True), ("09:29", True), ("09:30", False), ("14:00", False),
])
def test_flights_window_edges(hhmm, inside):
    hour, minute = map(int, hhmm.split(":"))
    assert watchdog.in_flights_window(NOW.replace(hour=hour, minute=minute)) is inside


# ------------------------------------------------------------ alert wording

def test_signature_ignores_ages_but_not_other_changes():
    assert watchdog._signature("flights are 57h stale") == watchdog._signature("flights are 59.5h stale")
    assert watchdog._signature("dead. Restarted Airflow automatically") != \
        watchdog._signature("dead. An automatic restart was already tried for this outage; needs a human")
    assert watchdog._signature("SFO weather is 4h stale") != watchdog._signature("SFO, LAX weather is 4h stale")


# ------------------------------------------------------------ scheduler check

@pytest.fixture
def jobs_check(monkeypatch):
    """Feed `airflow jobs check` a sequence of answers; no real waiting between attempts."""
    monkeypatch.setattr(watchdog.time, "sleep", lambda seconds: None)

    def feed(*answers):
        replies = iter(answers)

        def fake_run(*args, **kwargs):
            reply = next(replies)
            if isinstance(reply, Exception):
                raise reply
            return reply
        monkeypatch.setattr(watchdog.subprocess, "run", fake_run)
    return feed


DEAD = completed(1, stdout="No alive jobs found.")


def test_scheduler_alive(jobs_check):
    jobs_check(completed(0, stdout="Found one alive job."))
    assert watchdog.check_scheduler_alive("airflow", "/tmp") == (None, False)


def test_scheduler_dead_twice_is_definitely_dead(jobs_check):
    jobs_check(DEAD, DEAD)
    problem, definitely_dead = watchdog.check_scheduler_alive("airflow", "/tmp")
    assert definitely_dead and "scheduler is dead" in problem


def test_scheduler_recovering_on_retry_is_alive(jobs_check):
    jobs_check(DEAD, completed(0))
    assert watchdog.check_scheduler_alive("airflow", "/tmp") == (None, False)


@pytest.mark.parametrize("second", [
    completed(1, stderr="sqlalchemy.exc.OperationalError: connection to server at \"localhost\" failed"),
    subprocess.TimeoutExpired(cmd="airflow", timeout=120),
    completed(1, stdout=""),
])
def test_anything_but_two_clear_answers_is_inconclusive(jobs_check, second):
    # One "dead" plus a database error (Postgres down), a timeout or no output must never
    # count as dead: a restart there would fix nothing and could interrupt a working run.
    jobs_check(DEAD, second)
    problem, definitely_dead = watchdog.check_scheduler_alive("airflow", "/tmp")
    assert not definitely_dead and problem.startswith("scheduler check inconclusive")


# ------------------------------------------------------------ restart guards

@pytest.fixture
def systemctl(monkeypatch):
    """Record restart attempts instead of making them."""
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return completed(fake_run.returncode, stderr=fake_run.stderr)
    fake_run.returncode, fake_run.stderr = 0, ""
    monkeypatch.setattr(watchdog.subprocess, "run", fake_run)
    fake_run.calls = calls
    return fake_run


def test_first_restart_happens_and_is_recorded(systemctl):
    state = {}
    result = watchdog.restart_airflow_once("dead", state, NOW)
    assert result == "dead. Restarted Airflow automatically"
    assert systemctl.calls == [["sudo", "-n", "systemctl", "restart", "airflow.service"]]
    assert state["_restarted_this_outage"] is True
    assert state["_last_restart"] == NOW.isoformat()


def test_restart_is_saved_before_it_is_attempted(systemctl, monkeypatch):
    # If the watchdog died mid-restart, the next tick must still see the stamp.
    saved_before = []

    def fake_run(command, **kwargs):
        saved_before.append(watchdog._load_state().get("_last_restart"))
        return completed(0)
    monkeypatch.setattr(watchdog.subprocess, "run", fake_run)
    watchdog.restart_airflow_once("dead", {}, NOW)
    assert saved_before == [NOW.isoformat()]


def test_only_one_restart_per_outage(systemctl):
    state = {"_restarted_this_outage": True, "_last_restart": (NOW - timedelta(hours=10)).isoformat()}
    result = watchdog.restart_airflow_once("dead", state, NOW)
    assert "needs a human" in result and systemctl.calls == []


def test_no_restart_within_cooldown_even_for_a_new_outage(systemctl):
    state = {"_last_restart": (NOW - timedelta(hours=2, minutes=59)).isoformat()}
    result = watchdog.restart_airflow_once("dead", state, NOW)
    assert "already restarted automatically" in result and systemctl.calls == []


def test_restart_allowed_after_cooldown(systemctl):
    state = {"_last_restart": (NOW - timedelta(hours=3, minutes=1)).isoformat()}
    assert watchdog.restart_airflow_once("dead", state, NOW).endswith("Restarted Airflow automatically")
    assert len(systemctl.calls) == 1


def test_restart_deferred_in_flights_window_without_using_up_the_attempt(systemctl):
    state = {}
    result = watchdog.restart_airflow_once("dead", state, NOW.replace(hour=9, minute=5))
    assert "deferred" in result and systemctl.calls == []
    assert "_restarted_this_outage" not in state and "_last_restart" not in state


def test_failed_restart_is_reported_and_still_counts(systemctl):
    systemctl.returncode, systemctl.stderr = 1, "sudo: a password is required"
    state = {}
    result = watchdog.restart_airflow_once("dead", state, NOW)
    assert "Automatic restart FAILED: sudo: a password is required" in result
    assert state["_restarted_this_outage"] is True  # no retry loop on the next tick


def test_unreadable_last_restart_does_not_block_a_restart(systemctl):
    state = {"_last_restart": "not a timestamp"}
    assert watchdog.restart_airflow_once("dead", state, NOW).endswith("Restarted Airflow automatically")


# ------------------------------------------------------------ main(): one tick end to end

class FrozenDatetime(datetime):
    now_value = NOW

    @classmethod
    def now(cls, tz=None):
        return cls.now_value


@pytest.fixture
def tick(monkeypatch):
    """Run main() once with every outside check replaced. Returns what it posted and pinged."""
    world = {
        "service": None, "age": 60.0, "scheduler": (None, False), "dags": None, "freshness": None,
        "slack_ok": True, "posts": [], "heartbeats": [], "restarts": [], "scheduler_checked": 0,
    }

    def check_scheduler(*args):
        world["scheduler_checked"] += 1
        return world["scheduler"]

    def restart(detail, state, now):
        world["restarts"].append(detail)
        state["_restarted_this_outage"] = True
        return f"{detail}. Restarted Airflow automatically"

    def post(text):
        world["posts"].append(text)
        return world["slack_ok"]

    monkeypatch.setattr(watchdog, "datetime", FrozenDatetime)
    monkeypatch.setattr(watchdog, "check_airflow_service", lambda: world["service"])
    monkeypatch.setattr(watchdog, "minutes_since_airflow_started", lambda: world["age"])
    monkeypatch.setattr(watchdog, "check_scheduler_alive", check_scheduler)
    monkeypatch.setattr(watchdog, "restart_airflow_once", restart)
    monkeypatch.setattr(watchdog, "check_dag_health", lambda *args: world["dags"])
    monkeypatch.setattr(watchdog, "check_freshness", lambda: world["freshness"])
    monkeypatch.setattr(watchdog, "post", post)
    monkeypatch.setattr(watchdog, "heartbeat", lambda healthy: world["heartbeats"].append(healthy))

    def run(at=NOW):
        FrozenDatetime.now_value = at
        world["posts"].clear()
        world["heartbeats"].clear()
        watchdog.main()
        return world
    run.world = world
    return run


def test_healthy_tick_posts_nothing_and_pings_healthy(tick):
    world = tick()
    assert world["posts"] == [] and world["heartbeats"] == [True]


def test_young_service_is_not_checked_or_restarted(tick):
    tick.world["age"], tick.world["scheduler"] = 3.0, ("dead", True)
    world = tick()
    assert world["scheduler_checked"] == 0 and world["restarts"] == []


def test_dead_scheduler_is_restarted_and_reported(tick):
    tick.world["scheduler"] = ("airflow.service is active but its scheduler is dead", True)
    world = tick()
    assert len(world["restarts"]) == 1
    assert "Restarted Airflow automatically" in world["posts"][0] and world["heartbeats"] == [False]


def test_inconclusive_check_neither_restarts_nor_ends_the_outage(tick):
    watchdog._save_state({"_restarted_this_outage": True})
    tick.world["scheduler"] = ("scheduler check inconclusive: connection refused", False)
    world = tick()
    assert world["restarts"] == []
    assert watchdog._load_state().get("_restarted_this_outage") is True


def test_alive_scheduler_ends_the_outage(tick):
    watchdog._save_state({"_restarted_this_outage": True})
    tick()
    assert "_restarted_this_outage" not in watchdog._load_state()


def test_same_problem_is_not_re_alerted_within_six_hours(tick):
    tick.world["dags"] = "flight_pipeline_daily is missing"
    assert len(tick()["posts"]) == 1
    assert tick(NOW + timedelta(hours=5, minutes=59))["posts"] == []
    assert len(tick(NOW + timedelta(hours=6, minutes=1))["posts"]) == 1


def test_changed_wording_alerts_at_once(tick):
    tick.world["dags"] = "flight_pipeline_daily is missing"
    tick()
    tick.world["dags"] = "flight_pipeline_daily and weather_hourly are missing"
    assert len(tick(NOW + timedelta(minutes=30))["posts"]) == 1


def test_failed_slack_post_is_retried_next_tick(tick):
    tick.world["dags"], tick.world["slack_ok"] = "flight_pipeline_daily is missing", False
    tick()
    tick.world["slack_ok"] = True
    assert len(tick(NOW + timedelta(minutes=30))["posts"]) == 1


def test_recovery_is_announced_once(tick):
    tick.world["dags"] = "flight_pipeline_daily is missing"
    tick()
    tick.world["dags"] = None
    world = tick(NOW + timedelta(minutes=30))
    assert len(world["posts"]) == 1 and "recovered" in world["posts"][0] and world["heartbeats"] == [True]
    assert tick(NOW + timedelta(minutes=60))["posts"] == []


def test_recovery_retried_when_slack_is_down(tick):
    tick.world["dags"] = "flight_pipeline_daily is missing"
    tick()
    tick.world["dags"], tick.world["slack_ok"] = None, False
    tick(NOW + timedelta(minutes=30))
    tick.world["slack_ok"] = True
    world = tick(NOW + timedelta(minutes=60))
    assert len(world["posts"]) == 1 and "recovered" in world["posts"][0]


def test_freshness_problem_is_carried_between_checks(tick):
    tick.world["freshness"] = "flights are 57h stale"
    assert tick()["heartbeats"] == [False]
    # Not due again for 2 hours: the last result stands, so the heartbeat keeps failing
    # even though check_freshness would now say all is well.
    tick.world["freshness"] = None
    assert tick(NOW + timedelta(minutes=30))["heartbeats"] == [False]
    assert tick(NOW + timedelta(hours=2, minutes=1))["heartbeats"] == [True]
