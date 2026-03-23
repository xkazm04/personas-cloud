import type { Persona, PersonaToolDefinition, StructuredPrompt } from './types.js';
import { buildProtocolDocumentation } from './protocolRegistry.js';

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

  // Communication Protocols (generated from shared ProtocolRegistry)
  prompt += '## Communication Protocols\n\n';
  prompt += buildProtocolDocumentation();

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
): CliArgs {
  const args = [
    '-p', '-',
    '--output-format', 'stream-json',
    '--verbose',
    '--dangerously-skip-permissions',
  ];

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
  const envRemovals: string[] = ['CLAUDECODE', 'CLAUDE_CODE'];

  return { command: 'claude', args, envOverrides, envRemovals };
}
