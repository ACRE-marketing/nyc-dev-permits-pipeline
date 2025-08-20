# OIDC 无密钥方式：GitHub Actions → Google Sheets（追加历史）

## 一次性设置（Google Cloud）
1. 启用：**Google Sheets API**、**IAM Credentials API**。
2. 创建 **Service Account**（无需创建 key），例如：`gsheet-writer@PROJECT_ID.iam.gserviceaccount.com`。
3. 进入 Google Sheet，**将上面的服务账号邮箱添加为 Editor**（编辑权限）。
4. 创建 **Workload Identity Pool**（例如 `GITHUB_POOL`），Provider 选择 **GitHub**：
   - Issuer: `https://token.actions.githubusercontent.com`
   - Attribute mapping 至少包括：
     - `google.subject=assertion.sub`
     - `attribute.repository=assertion.repository`
     - `attribute.ref=assertion.ref`
5. 授权该 Provider **可冒充**上述服务账号：在 Service Account 的 IAM 中，
   给主体：
   `principalSet://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/GITHUB_POOL/attribute.repository/OWNER/REPO`
   赋予 **Workload Identity User** (`roles/iam.workloadIdentityUser`)。
6. 把 `cron.yml` 里 `workload_identity_provider` 和 `service_account` 两处替换成你的值。

## GitHub 仓库设置
- Secrets：
  - `GSHEET_ID`（必需）：你的表格 ID
  - `NYC_SODA_APP_TOKEN`（可选）：NYC Open Data token
- 触发：
  - `Actions → Run workflow` 手动跑一次（现在就可以）
  - 之后按 cron 定时执行（默认 09:00 ET）

## 本项目脚本
- `scraper_gsheet_oidc.py`：抓取 YIMBY / TRD / NYC DOB，并**追加**到 Google Sheet。
- 去重键：`(date, source, title, address)`；可在脚本中自行调整。
