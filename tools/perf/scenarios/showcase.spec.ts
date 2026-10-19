import { mkdir } from 'node:fs/promises';
import { expect, test } from '@playwright/test';
import { attachErrorSentry, openMonitor, sampleFirstRow, expectRowTicking } from '../lib/page';

test('capture the live monitor for the CI demo artifact', async ({ page }) => {
  const sentry = attachErrorSentry(page);
  await page.setViewportSize({ width: 1440, height: 960 });
  await openMonitor(page);
  const row = await sampleFirstRow(page);
  await expectRowTicking(page, row.id);
  await mkdir('report', { recursive: true });
  await page.screenshot({ path: 'report/monitor.png', fullPage: true });
  expect(sentry.errors).toEqual([]);
});
