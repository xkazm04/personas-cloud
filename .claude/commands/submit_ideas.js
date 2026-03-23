const http = require('http');

function request(options, bodyData) {
  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(JSON.parse(data));
        } else {
          reject(new Error(`Request failed with status ${res.statusCode}: ${data}`));
        }
      });
    });
    req.on('error', reject);
    if (bodyData) {
      req.write(JSON.stringify(bodyData));
    }
    req.end();
  });
}

async function run() {
  try {
    // 1. Create scan
    const scan = await request({
      hostname: 'localhost',
      port: 3000,
      path: '/api/scans',
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    }, {
      project_id: 'c6fd0474-19f1-4223-b6a9-752077c51d3d',
      scan_type: 'feature_scout',
      summary: 'Claude Code idea generation - Feature Scout'
    });

    const scanId = scan.scan ? scan.scan.id : scan.id;
    console.log(`Created scan with ID: ${scanId}`);

    // 2. Submit ideas
    const ideas = [
      {
        "category": "functionality",
        "title": "Persona Versioning and Rollbacks",
        "description": "Implement immutable versioning for Personas. When a persona is updated (system prompt, linked tools), create a new version instead of overwriting. Allow execution requests to target a specific version ID or default to 'latest', and add an endpoint to restore previous versions.",
        "reasoning": "Prompt engineering and tool configuration are highly iterative and fragile. Users need the safety to tweak a working persona and instantly revert if changes degrade performance, reducing the fear of breaking production workflows.",
        "effort": 6,
        "impact": 8,
        "risk": 3
      },
      {
        "category": "functionality",
        "title": "Global Tool Discovery Registry",
        "description": "Create a shared 'Tool Registry' for standard, pre-configured tool definitions (e.g., standard HTTP client, GitHub/Jira integrations). Add an 'isPublic' flag to PersonaToolDefinition and a clone endpoint to import registry tools directly into a user's project.",
        "reasoning": "Users currently waste time redefining common tools from scratch or copy-pasting schemas. A registry jump-starts persona creation, standardizes integrations, and accelerates onboarding by providing off-the-shelf capabilities.",
        "effort": 5,
        "impact": 7,
        "risk": 4
      },
      {
        "category": "user_benefit",
        "title": "Interactive Dry-Run Sandbox",
        "description": "Add a '/api/personas/:id/dry-run' endpoint that simulates execution using mock tool responses (defined via a new mockResponse field in tool definitions). This traces the persona's decision-making without hitting real external services or consuming actual budget.",
        "reasoning": "Developing agents that perform mutations (like updating records) is risky. A sandbox environment builds developer trust, allowing them to safely observe and iterate on the persona's logic before deploying it to production.",
        "effort": 7,
        "impact": 9,
        "risk": 5
      },
      {
        "category": "user_benefit",
        "title": "1-Click Persona Templates",
        "description": "Introduce built-in, immutable Persona templates (e.g., 'Code Reviewer', 'Data Analyst') that bundle proven system prompts, ideal inference profiles, and pre-linked tools. Implement a fork endpoint to instantly duplicate these templates into the user's active project.",
        "reasoning": "The 'blank canvas' problem makes initial onboarding difficult and intimidating. Templates provide immediate, tangible value, demonstrate platform capabilities, and drastically reduce the time-to-first-success for new users.",
        "effort": 4,
        "impact": 8,
        "risk": 2
      },
      {
        "category": "functionality",
        "title": "Strict Tool Schema Validation",
        "description": "Integrate JSON Schema validation (e.g., Ajv) into the tool definition POST/PUT endpoints to rigorously validate 'inputSchema' and 'outputSchema' fields on save. Reject invalid schemas immediately with descriptive error messages.",
        "reasoning": "Misconfigured schemas are a primary cause of agent failure, leading to hallucinated tool calls. Fast-failing at the configuration stage saves debugging time and ensures the LLM always receives perfectly formatted instructions.",
        "effort": 3,
        "impact": 6,
        "risk": 2
      }
    ];

    for (const idea of ideas) {
      await request({
        hostname: 'localhost',
        port: 3000,
        path: '/api/ideas',
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, {
        scan_id: scanId,
        project_id: 'c6fd0474-19f1-4223-b6a9-752077c51d3d',
        context_id: 'ctx_1773782578609_f81tdab',
        scan_type: 'feature_scout',
        ...idea
      });
      console.log(`Successfully created idea: ${idea.title}`);
    }

  } catch (err) {
    console.error('Error:', err.message);
  }
}

run();
