import { describe, it, afterEach } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { getDb } from "../dist/db.js";
import { completeStep, failStep } from "../dist/installer/step-ops.js";

describe("completeStep loop guard", () => {
  const testRunIds: string[] = [];

  afterEach(() => {
    const db = getDb();
    for (const runId of testRunIds) {
      db.prepare("DELETE FROM stories WHERE run_id = ?").run(runId);
      db.prepare("DELETE FROM steps WHERE run_id = ?").run(runId);
      db.prepare("DELETE FROM runs WHERE id = ?").run(runId);
    }
    testRunIds.length = 0;
  });

  it("does not advance pipeline when loop completion arrives without current_story_id", () => {
    const db = getDb();
    const runId = randomUUID();
    const loopStepId = randomUUID();
    const verifyStepId = randomUUID();
    const testStepId = randomUUID();
    const now = new Date().toISOString();

    db.prepare(
      `INSERT INTO runs (id, workflow_id, task, status, context, created_at, updated_at)
       VALUES (?, 'wf-loop-guard', 'test task', 'running', '{}', ?, ?)`
    ).run(runId, now, now);

    db.prepare(
      `INSERT INTO steps (id, step_id, run_id, agent_id, step_index, input_template, expects, status, created_at, updated_at, type, loop_config, current_story_id)
       VALUES (?, 'implement', ?, 'feature-dev', 2, '', 'IMPLEMENTED', 'running', ?, ?, 'loop', ?, NULL)`
    ).run(loopStepId, runId, now, now, JSON.stringify({ over: "stories", verifyEach: true, verifyStep: "verify" }));

    db.prepare(
      `INSERT INTO steps (id, step_id, run_id, agent_id, step_index, input_template, expects, status, created_at, updated_at, type)
       VALUES (?, 'verify', ?, 'feature-dev_verifier', 3, '', 'VERIFIED', 'pending', ?, ?, 'single')`
    ).run(verifyStepId, runId, now, now);

    db.prepare(
      `INSERT INTO steps (id, step_id, run_id, agent_id, step_index, input_template, expects, status, created_at, updated_at, type)
       VALUES (?, 'test', ?, 'feature-dev_tester', 4, '', 'TESTED', 'waiting', ?, ?, 'single')`
    ).run(testStepId, runId, now, now);

    testRunIds.push(runId);

    const res = completeStep(loopStepId, "STATUS: done");

    assert.equal(res.advanced, false);
    assert.equal(res.runCompleted, false);

    const loopStep = db.prepare("SELECT status, current_story_id FROM steps WHERE id = ?").get(loopStepId) as { status: string; current_story_id: string | null };
    assert.equal(loopStep.status, "running");
    assert.equal(loopStep.current_story_id, null);

    const verifyStep = db.prepare("SELECT status FROM steps WHERE id = ?").get(verifyStepId) as { status: string };
    assert.equal(verifyStep.status, "pending");

    const testStep = db.prepare("SELECT status FROM steps WHERE id = ?").get(testStepId) as { status: string };
    assert.equal(testStep.status, "waiting");
  });

  it("does not fail run when loop failure arrives without current_story_id", async () => {
    const db = getDb();
    const runId = randomUUID();
    const loopStepId = randomUUID();
    const verifyStepId = randomUUID();
    const now = new Date().toISOString();

    db.prepare(
      `INSERT INTO runs (id, workflow_id, task, status, context, created_at, updated_at)
       VALUES (?, 'wf-loop-guard', 'test task', 'running', '{}', ?, ?)`
    ).run(runId, now, now);

    db.prepare(
      `INSERT INTO steps (id, step_id, run_id, agent_id, step_index, input_template, expects, status, created_at, updated_at, type, loop_config, current_story_id, retry_count, max_retries)
       VALUES (?, 'implement', ?, 'feature-dev', 2, '', 'IMPLEMENTED', 'running', ?, ?, 'loop', ?, NULL, 1, 2)`
    ).run(loopStepId, runId, now, now, JSON.stringify({ over: "stories", verifyEach: true, verifyStep: "verify" }));

    db.prepare(
      `INSERT INTO steps (id, step_id, run_id, agent_id, step_index, input_template, expects, status, created_at, updated_at, type)
       VALUES (?, 'verify', ?, 'feature-dev_verifier', 3, '', 'VERIFIED', 'pending', ?, ?, 'single')`
    ).run(verifyStepId, runId, now, now);

    testRunIds.push(runId);

    const res = await failStep(loopStepId, "late loop failure");
    assert.equal(res.retrying, false);
    assert.equal(res.runFailed, false);

    const run = db.prepare("SELECT status FROM runs WHERE id = ?").get(runId) as { status: string };
    assert.equal(run.status, "running");

    const loopStep = db.prepare("SELECT status, retry_count FROM steps WHERE id = ?").get(loopStepId) as { status: string; retry_count: number };
    assert.equal(loopStep.status, "running");
    assert.equal(loopStep.retry_count, 1);

    const verifyStep = db.prepare("SELECT status FROM steps WHERE id = ?").get(verifyStepId) as { status: string };
    assert.equal(verifyStep.status, "pending");
  });
});
