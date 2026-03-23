// ---------------------------------------------------------------------------
// ProtocolRegistry — single source of truth for persona protocol events.
//
// prompt.ts consumes `buildProtocolDocumentation()` to emit LLM instructions.
// parser.ts consumes `PROTOCOL_EVENT_KEYS` to know which JSON keys to extract.
// All type-level union references use `ProtocolEventType`.
// ---------------------------------------------------------------------------

export interface ProtocolFieldDef {
  name: string;
  required: boolean;
  description: string;
}

export interface ProtocolEventDef {
  /** The JSON key that appears in agent output (e.g. "user_message"). */
  key: string;
  /** Human-readable heading for prompt documentation. */
  heading: string;
  /** One-line description of what this event does. */
  summary: string;
  /** Example JSON payload shown in the prompt. */
  example: string;
  /** Field documentation lines. */
  fields: ProtocolFieldDef[];
  /** Extra instructions appended after field docs (e.g. outcome_assessment rules). */
  extraInstructions?: string;
}

// ---------------------------------------------------------------------------
// Registry entries — add new protocol events here and everything updates.
// ---------------------------------------------------------------------------

const REGISTRY: readonly ProtocolEventDef[] = [
  {
    key: 'user_message',
    heading: 'User Message Protocol',
    summary: 'To send a message to the user, output a JSON object on its own line:',
    example: '{"user_message": {"title": "Optional Title", "content": "Message content here", "content_type": "info", "priority": "normal"}}',
    fields: [
      { name: 'title', required: false, description: 'Short title for the message' },
      { name: 'content', required: true, description: 'The message body' },
      { name: 'content_type', required: false, description: '"info", "warning", "error", "success" (default: "info")' },
      { name: 'priority', required: false, description: '"low", "normal", "high", "urgent" (default: "normal")' },
    ],
  },
  {
    key: 'persona_action',
    heading: 'Persona Action Protocol',
    summary: 'To trigger an action on another persona, output a JSON object on its own line:',
    example: '{"persona_action": {"target": "target-persona-id", "action": "run", "input": {"key": "value"}}}',
    fields: [
      { name: 'target', required: true, description: 'The persona ID to target' },
      { name: 'action', required: false, description: 'Action to perform (default: "run")' },
      { name: 'input', required: false, description: 'JSON data to pass to the target persona' },
    ],
  },
  {
    key: 'emit_event',
    heading: 'Emit Event Protocol',
    summary: 'To emit an event to the system event bus, output a JSON object on its own line:',
    example: '{"emit_event": {"type": "task_completed", "data": {"result": "success", "details": "..."}}}',
    fields: [
      { name: 'type', required: true, description: 'Event type identifier' },
      { name: 'data', required: false, description: 'Arbitrary JSON payload' },
    ],
  },
  {
    key: 'agent_memory',
    heading: 'Agent Memory Protocol',
    summary: 'To store a memory for future reference, output a JSON object on its own line:',
    example: '{"agent_memory": {"title": "Memory Title", "content": "What to remember", "category": "learning", "importance": 5, "tags": ["tag1", "tag2"]}}',
    fields: [
      { name: 'title', required: true, description: 'Short title for the memory' },
      { name: 'content', required: true, description: 'Detailed content to remember' },
      { name: 'category', required: false, description: '"learning", "preference", "fact", "procedure" (default: "general")' },
      { name: 'importance', required: false, description: '1-10 importance rating (default: 5)' },
      { name: 'tags', required: false, description: 'Array of string tags for categorization' },
    ],
  },
  {
    key: 'manual_review',
    heading: 'Manual Review Protocol',
    summary: 'To flag something for human review, output a JSON object on its own line:',
    example: '{"manual_review": {"title": "Review Title", "description": "What needs review", "severity": "medium", "context_data": "relevant context", "suggested_actions": ["action1", "action2"]}}',
    fields: [
      { name: 'title', required: true, description: 'Short title describing the review item' },
      { name: 'description', required: false, description: 'Detailed description' },
      { name: 'severity', required: false, description: '"low", "medium", "high", "critical" (default: "medium")' },
      { name: 'context_data', required: false, description: 'Additional context string' },
      { name: 'suggested_actions', required: false, description: 'Array of suggested resolution steps' },
    ],
  },
  {
    key: 'execution_flow',
    heading: 'Execution Flow Protocol',
    summary: 'To declare execution flow metadata, output a JSON object on its own line:',
    example: '{"execution_flow": {"flows": [{"step": 1, "action": "analyze", "status": "completed"}, {"step": 2, "action": "implement", "status": "pending"}]}}',
    fields: [
      { name: 'flows', required: true, description: 'JSON value describing the execution flow steps' },
    ],
  },
  {
    key: 'outcome_assessment',
    heading: 'Outcome Assessment Protocol',
    summary: 'IMPORTANT: At the very end of your execution, you MUST output an outcome assessment as the last thing before finishing:',
    example: '{"outcome_assessment": {"accomplished": true, "summary": "Brief description of what was achieved"}}',
    fields: [
      { name: 'accomplished', required: true, description: 'true if the task was successfully completed from a business perspective, false if it could not be completed' },
      { name: 'summary', required: true, description: 'Brief description of the outcome' },
      { name: 'blockers', required: false, description: 'List of reasons the task could not be completed (only when accomplished is false)' },
    ],
    extraInstructions: `You MUST always output this assessment. Set accomplished to false if:
- Required data was not available or accessible
- External services were unreachable or returned errors that prevented task completion
- The task requirements could not be fulfilled with the available tools
- You could not verify the task was completed correctly`,
  },
] as const;

// ---------------------------------------------------------------------------
// Derived exports
// ---------------------------------------------------------------------------

/** All protocol event keys as a readonly tuple. */
export const PROTOCOL_EVENT_KEYS = REGISTRY.map(e => e.key) as readonly string[];

/** Union type of all protocol event keys. */
export type ProtocolEventType =
  | 'user_message'
  | 'persona_action'
  | 'emit_event'
  | 'agent_memory'
  | 'manual_review'
  | 'execution_flow'
  | 'outcome_assessment';

/** Access the full registry (e.g. for tooling or tests). */
export function getProtocolRegistry(): readonly ProtocolEventDef[] {
  return REGISTRY;
}

// ---------------------------------------------------------------------------
// Prompt generation — replaces hand-coded string constants in prompt.ts
// ---------------------------------------------------------------------------

function buildEventDoc(def: ProtocolEventDef): string {
  let doc = `### ${def.heading}\n`;
  doc += `${def.summary}\n`;
  doc += '```json\n';
  doc += `${def.example}\n`;
  doc += '```\n';
  doc += 'Fields:\n';
  for (const f of def.fields) {
    const req = f.required ? 'required' : 'optional';
    doc += `- \`${f.name}\` (${req}): ${f.description}\n`;
  }
  if (def.extraInstructions) {
    doc += '\n' + def.extraInstructions + '\n';
  }
  doc += '\n';
  return doc;
}

/** Generate the full "Communication Protocols" prompt section from the registry. */
export function buildProtocolDocumentation(): string {
  return REGISTRY.map(buildEventDoc).join('');
}
