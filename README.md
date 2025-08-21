# NYC Dev & DOB → CSV (for Google Sheets pull)

最简单的方案：GitHub 每天生成 CSV 并提交到仓库，Google Sheet 用 Apps Script **按天拉取 + 追加**。无需服务账号/密钥。

## 使用步骤
1. 把本包里的文件上传到你的 GitHub 仓库根目录（保留 `.github/workflows/` 路径）。
   - 如之前用了 OIDC 方案，可删除 `scraper_gsheet_oidc.py` 与旧的 `.github/workflows/cron.yml`。
2. （可选）在仓库 **Settings → Secrets → Actions** 设置 `NYC_SODA_APP_TOKEN`。
3. 打开 **Actions → Run workflow** 手动跑一次，仓库会生成：`public/nyc_developers_daily.csv`。

## Google Sheet 侧（Apps Script）
在你的 Google Sheet → **Extensions → Apps Script** 新建脚本，粘贴 `apps_script_import.js` 内容，修改 `RAW_CSV_URL` 为：
```
https://raw.githubusercontent.com/OWNER/REPO/BRANCH/public/nyc_developers_daily.csv
```
（例如 `.../main/public/nyc_developers_daily.csv`，仓库需 Public 才能直接访问）

保存后点击 **Run** 验证成功，再在 Apps Script 左侧 **Triggers** 设定每日定时（如 09:10 ET）。
