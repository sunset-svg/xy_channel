import os from "node:os";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  log: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
  sendMessage: vi.fn(),
}));

vi.mock("../src/transport/client.js", () => ({
  getXYWebSocketManager: () => ({ sendMessage: mocks.sendMessage }),
}));

vi.mock("../src/transport/push.js", () => ({ XYPushService: class {} }));
vi.mock("../src/utils/pushid-manager.js", () => ({ getAllPushIds: vi.fn() }));
vi.mock("../src/utils/logger.js", () => ({
  logger: { withContext: () => mocks },
  isA2AOutLogEnabled: () => false,
}));

import { sendWsFrame, type SendWsFrameParams } from "../src/conversation/outbound-gateway.js";

const MAX_FRAME_BYTES = 64 * 1024;
const sessionId = "session-cronquery";
const taskId = "task";
const agentId = "agent";

function makeReply(entries: { runId: string; summary: string }[], offset = 0) {
  const answer = {
    entries,
    total: offset + entries.length,
    offset,
    limit: entries.length,
    hasMore: false,
    nextOffset: null,
  };
  const command = {
    header: { namespace: "AgentEvent", name: "CronQuery" },
    payload: { action: "runs", status: true, ans: answer },
  };
  const payload = {
    jsonrpc: "2.0",
    id: "message",
    result: {
      kind: "artifact-update",
      final: true,
      artifact: {
        artifactId: "artifact",
        parts: [{ kind: "data", data: { commands: [command] } }],
      },
    },
  };
  return { payload, command, answer };
}

function sendReply(payload: SendWsFrameParams["payload"], targetSessionId = sessionId) {
  return sendWsFrame({ config: { agentId } as SendWsFrameParams["config"], sessionId: targetSessionId, taskId, payload });
}

function readSentReply() {
  expect(mocks.sendMessage).toHaveBeenCalledTimes(1);
  const message = mocks.sendMessage.mock.calls[0][1];
  const reply = JSON.parse(message.msgDetail);
  return {
    bytes: Buffer.byteLength(JSON.stringify(message), "utf8"),
    command: reply.result.artifact.parts[0].data.commands[0],
    final: reply.result.final,
  };
}

describe("CronQuery.runs outbound WebSocket size limit", () => {
  beforeEach(() => vi.clearAllMocks());

  it("preserves a response at exactly 64 KiB", async () => {
    const entry = { runId: "1", summary: "" };
    const { payload, answer } = makeReply([entry]);
    const emptyFrame = {
      msgType: "agent_response",
      agentId,
      sessionId,
      taskId,
      msgDetail: JSON.stringify({ ...payload, hostname: os.hostname() }),
    };
    entry.summary = "x".repeat(MAX_FRAME_BYTES - Buffer.byteLength(JSON.stringify(emptyFrame), "utf8"));

    await sendReply(payload);

    const sent = readSentReply();
    expect(sent.bytes).toBe(MAX_FRAME_BYTES);
    expect(sent.command.payload.ans).toEqual(answer);
    expect(mocks.log).toHaveBeenCalledWith(expect.stringContaining("outcome=unchanged"));
  });

  it("fits the largest prefix using UTF-8 and the outer envelope and advances by entries sent", async () => {
    const entries = ["1", "2", "3"].map((runId) => ({ runId, summary: '\n"中'.repeat(2800) }));
    const { payload, answer } = makeReply(entries, 20);

    await sendReply(payload);

    const sent = readSentReply();
    expect(sent.bytes).toBeLessThanOrEqual(MAX_FRAME_BYTES);
    expect(sent.final).toBe(true);
    expect(sent.command.payload.ans).toEqual({
      entries: entries.slice(0, 2), total: 23, offset: 20, limit: 2, hasMore: true, nextOffset: 22,
    });
    expect(answer.entries).toHaveLength(3);
    expect(answer.nextOffset).toBeNull();
    expect(mocks.warn).toHaveBeenCalledWith(expect.stringContaining(`outcome=resized sentEntries=2 sentBytes=${sent.bytes} nextOffset=22`));
  });

  it("sends a bounded error when the first entry is too large to fit", async () => {
    const { payload } = makeReply([{ runId: "1", summary: "中".repeat(30_000) }, { runId: "2", summary: "ok" }]);

    await sendReply(payload);

    const sent = readSentReply();
    expect(sent.bytes).toBeLessThan(MAX_FRAME_BYTES);
    expect(sent.command.payload).toEqual({
      action: "runs",
      status: false,
      ans: { error: expect.stringContaining("64 KiB") },
    });
    expect(mocks.warn).toHaveBeenCalledWith(expect.stringContaining(`outcome=error sentBytes=${sent.bytes}`));
  });

  it("rejects the send if the envelope is too large even for the error", async () => {
    const { payload } = makeReply([]);

    await expect(sendReply(payload, "s".repeat(MAX_FRAME_BYTES))).rejects.toThrow("64 KiB");

    expect(mocks.sendMessage).not.toHaveBeenCalled();
    expect(mocks.error).toHaveBeenCalledWith(expect.stringContaining("outcome=rejected"));
  });

  it("leaves CronQuery.list on its existing response path", async () => {
    const { payload, command } = makeReply([]);
    command.payload.action = "list";

    await sendReply(payload);

    expect(readSentReply().command).toEqual(command);
    expect(mocks.log).not.toHaveBeenCalled();
    expect(mocks.warn).not.toHaveBeenCalled();
  });
});
