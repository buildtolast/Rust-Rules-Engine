import { test, expect } from '@playwright/test';

const mockRules = [
  {
    id: '1',
    description: 'High Value Transaction',
    spelExpression: "payload.amount > 1000",
    active: true,
    updatedAt: '2026-06-17T07:00:00Z'
  },
  {
    id: '2',
    description: 'EU Region Check',
    spelExpression: "payload.region == 'EU'",
    active: false,
    updatedAt: '2026-06-17T07:05:00Z'
  }
];

test.describe('Rules Engine Dashboard UI', () => {
  test.beforeEach(async ({ page }) => {
    // Mock API requests
    await page.route('/api/rules', async (route) => {
      if (route.request().method() === 'GET') {
        await route.fulfill({ json: mockRules });
      } else if (route.request().method() === 'POST') {
        const body = route.request().postDataJSON();
        await route.fulfill({ json: { ...body, id: '3', updatedAt: new Date().toISOString() } });
      }
    });

    await page.route('/api/rules/1', async (route) => {
      if (route.request().method() === 'PUT') {
        const body = route.request().postDataJSON();
        await route.fulfill({ json: { ...body, updatedAt: new Date().toISOString() } });
      } else if (route.request().method() === 'DELETE') {
        await route.fulfill({ status: 200 });
      }
    });

    await page.goto('/');
  });

  test('should display existing rules', async ({ page }) => {
    await expect(page.locator('h1')).toHaveText('Rules Engine Dashboard');
    await expect(page.getByText('High Value Transaction')).toBeVisible();
    await expect(page.getByText('EU Region Check')).toBeVisible();
    await expect(page.getByText('payload.amount > 1000')).toBeVisible();
  });

  test('should open create modal and save new rule', async ({ page }) => {
    await page.getByRole('button', { name: 'Add New Rule' }).click();
    
    await expect(page.getByText('Create Rule')).toBeVisible();
    
    await page.fill('#description', 'New Rule Description');
    await page.fill('#expression', 'payload.id != null');
    
    // Intercept the POST request to verify it's called
    const postRequest = page.waitForRequest(request => 
      request.url().includes('/api/rules') && request.method() === 'POST'
    );
    
    await page.getByRole('button', { name: 'Save Rule' }).click();
    
    const request = await postRequest;
    expect(request.postDataJSON()).toMatchObject({
      description: 'New Rule Description',
      spelExpression: 'payload.id != null',
      active: true
    });
    
    // Modal should be closed
    await expect(page.getByText('Create Rule')).not.toBeVisible();
  });

  test('should open edit modal and update rule', async ({ page }) => {
    // Click edit on the first rule
    await page.locator('tr').filter({ hasText: 'High Value Transaction' }).getByTitle('Edit rule').click();
    
    await expect(page.getByText('Edit Rule')).toBeVisible();
    await expect(page.locator('#description')).toHaveValue('High Value Transaction');
    
    await page.fill('#description', 'Updated Transaction Rule');
    
    const putRequest = page.waitForRequest(request => 
      request.url().includes('/api/rules/1') && request.method() === 'PUT'
    );
    
    await page.getByRole('button', { name: 'Update Rule' }).click();
    
    const request = await putRequest;
    expect(request.postDataJSON().description).toBe('Updated Transaction Rule');
  });

  test('should delete a rule', async ({ page }) => {
    // Mock window.confirm
    page.on('dialog', dialog => dialog.accept());
    
    const deleteRequest = page.waitForRequest(request => 
      request.url().includes('/api/rules/1') && request.method() === 'DELETE'
    );
    
    await page.locator('tr').filter({ hasText: 'High Value Transaction' }).getByTitle('Delete rule').click();
    
    await deleteRequest;
  });

  test('should display error message on API failure', async ({ page }) => {
    // Override the GET /api/rules mock to fail
    await page.route('/api/rules', async (route) => {
      if (route.request().method() === 'GET') {
        await route.fulfill({ status: 500, body: 'Internal Server Error' });
      }
    });

    await page.goto('/');
    
    await expect(page.getByText('Failed to fetch rules')).toBeVisible();
    await expect(page.getByText('Error')).toBeVisible();
  });
});
