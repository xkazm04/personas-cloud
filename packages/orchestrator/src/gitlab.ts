import type { Logger } from 'pino';

/**
 * Trigger a GitLab CI/CD pipeline via the API.
 * Used for outbound GitLab integration — when a persona execution
 * needs to trigger a pipeline in a GitLab project.
 */
export async function triggerGitLabPipeline(
  gitlabUrl: string,
  projectId: string | number,
  token: string,
  ref: string,
  variables: Record<string, string>,
  logger: Logger,
): Promise<{ pipelineId: number; webUrl: string }> {
  const encodedProject = encodeURIComponent(String(projectId));
  const url = `${gitlabUrl}/api/v4/projects/${encodedProject}/trigger/pipeline`;

  const formData = new URLSearchParams();
  formData.set('token', token);
  formData.set('ref', ref);

  for (const [key, value] of Object.entries(variables)) {
    formData.set(`variables[${key}]`, value);
  }

  const response = await fetch(url, {
    method: 'POST',
    body: formData,
  });

  if (!response.ok) {
    const text = await response.text();
    logger.error({ status: response.status, body: text, projectId }, 'GitLab pipeline trigger failed');
    throw new Error(`GitLab API error ${response.status}: ${text}`);
  }

  const data = await response.json() as { id: number; web_url: string };

  logger.info({ pipelineId: data.id, projectId, ref }, 'GitLab pipeline triggered');

  return {
    pipelineId: data.id,
    webUrl: data.web_url,
  };
}

/**
 * Map a GitLab webhook event kind to a persona event type.
 */
export function gitlabEventType(objectKind: string): string {
  const mapping: Record<string, string> = {
    push: 'gitlab_push',
    merge_request: 'gitlab_mr',
    pipeline: 'gitlab_pipeline',
    tag_push: 'gitlab_tag',
    issue: 'gitlab_issue',
    note: 'gitlab_comment',
    build: 'gitlab_build',
  };
  return mapping[objectKind] ?? `gitlab_${objectKind}`;
}
