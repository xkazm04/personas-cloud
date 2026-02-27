import type { Persona, PersonaToolDefinition, StructuredPrompt, ModelProfile } from './types.js';

// ---------------------------------------------------------------------------
// Protocol instruction constants (matching desktop engine/prompt.rs)
// ---------------------------------------------------------------------------

const PROTOCOL_USER_MESSAGE = `### User Message Protocol
To send a message to the user, output a JSON object on its own line:
\`\`\`json
{"user_message": {"title": "Optional Title", "content": "Message content here", "content_type": "info", "priority": "normal"}}
\`\`\`
Fields:
- \`title\` (optional): Short title for the message
- \`content\` (required): The message body
- \`content_type\` (optional): "info", "warning", "error", "success" (default: "info")
- \`priority\` (optional): "low", "normal", "high", "urgent" (default: "normal")

`;

const PROTOCOL_PERSONA_ACTION = `### Persona Action Protocol
To trigger an action on another persona, output a JSON object on its own line:
\`\`\`json
{"persona_action": {"target": "target-persona-id", "action": "run", "input": {"key": "value"}}}
\`\`\`
Fields:
- \`target\` (required): The persona ID to target
- \`action\` (optional): Action to perform (default: "run")
- \`input\` (optional): JSON data to pass to the target persona

`;

const PROTOCOL_EMIT_EVENT = `### Emit Event Protocol
To emit an event to the system event bus, output a JSON object on its own line:
\`\`\`json
{"emit_event": {"type": "task_completed", "data": {"result": "success", "details": "..."}}}
\`\`\`
Fields:
- \`type\` (required): Event type identifier
- \`data\` (optional): Arbitrary JSON payload

`;

const PROTOCOL_AGENT_MEMORY = `### Agent Memory Protocol
To store a memory for future reference, output a JSON object on its own line:
\`\`\`json
{"agent_memory": {"title": "Memory Title", "content": "What to remember", "category": "learning", "importance": 5, "tags": ["tag1", "tag2"]}}
\`\`\`
Fields:
- \`title\` (required): Short title for the memory
- \`content\` (required): Detailed content to remember
- \`category\` (optional): "learning", "preference", "fact", "procedure" (default: "general")
- \`importance\` (optional): 1-10 importance rating (default: 5)
- \`tags\` (optional): Array of string tags for categorization

`;

const PROTOCOL_MANUAL_REVIEW = `### Manual Review Protocol
To flag something for human review, output a JSON object on its own line:
\`\`\`json
{"manual_review": {"title": "Review Title", "description": "What needs review", "severity": "medium", "context_data": "relevant context", "suggested_actions": ["action1", "action2"]}}
\`\`\`
Fields:
- \`title\` (required): Short title describing the review item
- \`description\` (optional): Detailed description
- \`severity\` (optional): "low", "medium", "high", "critical" (default: "medium")
- \`context_data\` (optional): Additional context string
- \`suggested_actions\` (optional): Array of suggested resolution steps

`;

const PROTOCOL_EXECUTION_FLOW = `### Execution Flow Protocol
To declare execution flow metadata, output a JSON object on its own line:
\`\`\`json
{"execution_flow": {"flows": [{"step": 1, "action": "analyze", "status": "completed"}, {"step": 2, "action": "implement", "status": "pending"}]}}
\`\`\`
Fields:
- \`flows\` (required): JSON value describing the execution flow steps

`;

const PROTOCOL_OUTCOME_ASSESSMENT = `### Outcome Assessment Protocol
IMPORTANT: At the very end of your execution, you MUST output an outcome assessment as the last thing before finishing:
\`\`\`json
{"outcome_assessment": {"accomplished": true, "summary": "Brief description of what was achieved"}}
\`\`\`
Fields:
- \`accomplished\` (required): true if the task was successfully completed from a business perspective, false if it could not be completed
- \`summary\` (required): Brief description of the outcome
- \`blockers\` (optional): List of reasons the task could not be completed (only when accomplished is false)

You MUST always output this assessment. Set accomplished to false if:
- Required data was not available or accessible
- External services were unreachable or returned errors that prevented task completion
- The task requirements could not be fulfilled with the available tools
- You could not verify the task was completed correctly

`;

// ---------------------------------------------------------------------------
// Tool documentation
// ---------------------------------------------------------------------------

export function buildToolDocumentation(tool: PersonaToolDefinition): string {
  let doc = `### ${tool.name}\n${tool.description}\n`;
  doc += `**Category**: ${tool.category}\n`;

  if (!tool.scriptPath) {
    if (tool.implementationGuide) {
      doc += '**Implementation Guide**:\n';
      doc += tool.implementationGuide;
      doc += '\n';
    } else {
      doc += '**Implementation**: Use the Bash tool with `curl` to call the API. Credentials are available as environment variables (e.g. `$GOOGLE_ACCESS_TOKEN`).\n';
    }
  } else {
    doc += `**Usage**: npx tsx "${tool.scriptPath}" --input '<JSON>'\n`;
  }

  if (tool.inputSchema) {
    doc += `**Input Schema**: ${tool.inputSchema}\n`;
  }
  if (tool.requiresCredentialType) {
    doc += `**Requires Credential**: ${tool.requiresCredentialType} (available as env var)\n`;
  }
  return doc;
}

// ---------------------------------------------------------------------------
// Prompt assembly (ported from engine/prompt.rs)
// ---------------------------------------------------------------------------

export function assemblePrompt(
  persona: Persona,
  tools: PersonaToolDefinition[],
  inputData?: Record<string, unknown>,
  credentialHints?: string[],
): string {
  let prompt = '';

  // Header
  prompt += `# Persona: ${persona.name}\n\n`;

  // Description
  if (persona.description) {
    prompt += '## Description\n';
    prompt += persona.description;
    prompt += '\n\n';
  }

  // Identity and Instructions from structured_prompt or system_prompt
  let usedStructured = false;
  if (persona.structuredPrompt) {
    try {
      const sp: StructuredPrompt = JSON.parse(persona.structuredPrompt);
      usedStructured = true;

      if (sp.identity) {
        prompt += '## Identity\n';
        prompt += sp.identity;
        prompt += '\n\n';
      }

      if (sp.instructions) {
        prompt += '## Instructions\n';
        prompt += sp.instructions;
        prompt += '\n\n';
      }

      if (sp.toolGuidance) {
        prompt += '## Tool Guidance\n';
        prompt += sp.toolGuidance;
        prompt += '\n\n';
      }

      if (sp.examples) {
        prompt += '## Examples\n';
        prompt += sp.examples;
        prompt += '\n\n';
      }

      if (sp.errorHandling) {
        prompt += '## Error Handling\n';
        prompt += sp.errorHandling;
        prompt += '\n\n';
      }

      if (sp.customSections) {
        for (const section of sp.customSections) {
          const heading = section.title ?? section.label ?? section.name ?? section.key;
          if (heading && section.content) {
            prompt += `## ${heading}\n`;
            prompt += section.content;
            prompt += '\n\n';
          }
        }
      }

      if (sp.webSearch) {
        prompt += '## Web Search Research Prompt\n';
        prompt += 'When performing web searches during this execution, use the following research guidance:\n\n';
        prompt += sp.webSearch;
        prompt += '\n\n';
      }
    } catch {
      usedStructured = false;
    }
  }

  if (!usedStructured) {
    prompt += '## Identity\n';
    prompt += persona.systemPrompt;
    prompt += '\n\n';
  }

  // Available Tools
  if (tools.length > 0) {
    prompt += '## Available Tools\n';
    for (const tool of tools) {
      prompt += buildToolDocumentation(tool);
      prompt += '\n';
    }
  }

  // Execution Environment (always Linux for cloud)
  prompt += '## Execution Environment\n';
  prompt += '- Platform: Linux/macOS\n';
  prompt += '- Available: `curl`, `node`, `npx`, `git`, `bash`\n';
  prompt += '- PREFER `curl` for HTTP API calls — avoid writing scripts when a single curl command works\n';
  prompt += '- Credentials are pre-injected as environment variables — access them with `$ENV_VAR_NAME` in curl commands\n\n';

  // Available Credentials
  if (credentialHints && credentialHints.length > 0) {
    prompt += '## Available Credentials (as environment variables)\n';
    prompt += 'These env vars are ALREADY SET in your shell — use them directly in curl commands:\n';
    for (const hint of credentialHints) {
      prompt += `- ${hint}\n`;
    }
    prompt += '\nExample: `curl -H "Authorization: Bearer $GOOGLE_ACCESS_TOKEN" https://api.example.com`\n';
    prompt += 'IMPORTANT: Do NOT check if env vars exist — they are pre-configured. Just use them.\n\n';
  }

  // Communication Protocols
  prompt += '## Communication Protocols\n\n';
  prompt += PROTOCOL_USER_MESSAGE;
  prompt += PROTOCOL_PERSONA_ACTION;
  prompt += PROTOCOL_EMIT_EVENT;
  prompt += PROTOCOL_AGENT_MEMORY;
  prompt += PROTOCOL_MANUAL_REVIEW;
  prompt += PROTOCOL_EXECUTION_FLOW;
  prompt += PROTOCOL_OUTCOME_ASSESSMENT;

  // Input Data
  if (inputData) {
    // Inject use case context if present
    const useCase = inputData['_use_case'] as Record<string, unknown> | undefined;
    if (useCase) {
      prompt += '## Use Case Context\n';
      if (typeof useCase['title'] === 'string') {
        prompt += `You are executing the use case: **${useCase['title']}**\n`;
      }
      if (typeof useCase['description'] === 'string') {
        prompt += `Description: ${useCase['description']}\n`;
      }
      prompt += 'Focus on this specific use case.\n\n';
    }

    // Inject time filter constraints
    const timeFilter = inputData['_time_filter'] as Record<string, unknown> | undefined;
    if (timeFilter) {
      prompt += '## Time Filter (IMPORTANT)\n';
      if (typeof timeFilter['description'] === 'string') {
        prompt += `${timeFilter['description']}\n`;
      }
      if (typeof timeFilter['field'] === 'string' && typeof timeFilter['default_window'] === 'string') {
        prompt += `When querying data, use the '${timeFilter['field']}' parameter to limit results to the last ${timeFilter['default_window']}. `;
        prompt += 'Do NOT fetch all historical data — only process recent items within this time window.\n';
      }
      prompt += '\n';
    }

    prompt += '## Input Data\n```json\n';
    prompt += JSON.stringify(inputData, null, 2);
    prompt += '\n```\n\n';
  }

  // Execute Now
  prompt += '## EXECUTE NOW\n';
  prompt += `You are ${persona.name}. Execute your task now. Follow your instructions precisely.\n`;
  if (tools.length > 0) {
    prompt += 'Use available tools as needed.\n';
  }
  prompt += 'Respond naturally and complete the task.\n';

  return prompt;
}

// ---------------------------------------------------------------------------
// CLI args builder (ported from engine/prompt.rs)
// ---------------------------------------------------------------------------

export interface CliArgs {
  command: string;
  args: string[];
  envOverrides: Array<[string, string]>;
  envRemovals: string[];
}

export function buildCliArgs(
  persona?: Persona | null,
  modelProfile?: ModelProfile | null,
): CliArgs {
  const args = [
    '-p', '-',
    '--output-format', 'stream-json',
    '--verbose',
    '--dangerously-skip-permissions',
  ];

  // Model override
  if (modelProfile?.model) {
    args.push('--model', modelProfile.model);
  }

  // Persona-specific flags
  if (persona) {
    if (persona.maxBudgetUsd && persona.maxBudgetUsd > 0) {
      args.push('--max-budget-usd', String(persona.maxBudgetUsd));
    }
    if (persona.maxTurns && persona.maxTurns > 0) {
      args.push('--max-turns', String(persona.maxTurns));
    }
  }

  const envOverrides: Array<[string, string]> = [];
  const envRemovals: string[] = [];

  // Provider-specific env
  if (modelProfile?.provider) {
    switch (modelProfile.provider) {
      case 'ollama':
        if (modelProfile.baseUrl) envOverrides.push(['OLLAMA_BASE_URL', modelProfile.baseUrl]);
        if (modelProfile.authToken) envOverrides.push(['OLLAMA_API_KEY', modelProfile.authToken]);
        envRemovals.push('ANTHROPIC_API_KEY');
        break;
      case 'litellm':
        if (modelProfile.baseUrl) envOverrides.push(['ANTHROPIC_BASE_URL', modelProfile.baseUrl]);
        if (modelProfile.authToken) envOverrides.push(['ANTHROPIC_AUTH_TOKEN', modelProfile.authToken]);
        envRemovals.push('ANTHROPIC_API_KEY');
        break;
      case 'custom':
        if (modelProfile.baseUrl) envOverrides.push(['OPENAI_BASE_URL', modelProfile.baseUrl]);
        if (modelProfile.authToken) envOverrides.push(['OPENAI_API_KEY', modelProfile.authToken]);
        envRemovals.push('ANTHROPIC_API_KEY');
        break;
    }
  }

  envRemovals.push('CLAUDECODE', 'CLAUDE_CODE');

  return { command: 'claude', args, envOverrides, envRemovals };
}

export function parseModelProfile(json: string | null | undefined): ModelProfile | null {
  if (!json?.trim()) return null;
  try {
    return JSON.parse(json) as ModelProfile;
  } catch {
    return null;
  }
}
