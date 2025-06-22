---

### **完整 GitHub 推送流程（远程分支为 `master`）**

#### 1. **初始化 Git 仓库**
如果项目目录尚未初始化为 Git 仓库，执行：
```powershell
cd /e:/self_test/agent_program/000-建模/000-通用
git init
```

#### 2. **添加远程仓库**
将 GitHub 仓库添加为远程仓库（命名为 `origin`）：
```powershell
git remote add origin https://github.com/zh2673-git/datadone.git
```

#### 3. **检查远程仓库配置**
确认远程仓库是否已正确添加：
```powershell
git remote -v
```
输出应显示：
```
origin  https://github.com/zh2673-git/datadone.git (fetch)
origin  https://github.com/zh2673-git/datadone.git (push)
```

#### 4. **检查本地分支**
查看本地分支名称：
```powershell
git branch
```
- 如果输出为 `master`，直接继续下一步。
- 如果输出为 `main`，重命名为 `master`：
  ```powershell
  git branch -m main master
  ```

#### 5. **添加文件到暂存区**
将所有文件添加到暂存区：
```powershell
git add .
```

#### 6. **提交更改**
提交暂存区的文件到本地仓库：
```powershell
git commit -m "Initial commit: project setup"
```

#### 7. **推送至 GitHub**
将本地 `master` 分支推送到远程 `master` 分支：
```powershell
git push -u origin master
```

#### 8. **验证推送**
打开 GitHub 仓库页面 `https://github.com/zh2673-git/datadone`，确认文件已成功上传。

---

### **注意事项**
1. **GitHub 凭据**：
   - 如果提示输入用户名和密码，请使用 GitHub 的 **个人访问令牌（PAT）** 代替密码。
   - 生成令牌的步骤：
     1. 访问 [GitHub Settings > Developer Settings > Personal Access Tokens](https://github.com/settings/tokens)。
     2. 创建新令牌，勾选 `repo` 权限。
     3. 复制令牌并粘贴到密码输入框。

2. **强制推送（仅限空仓库）**：
   - 如果远程仓库是空的且推送失败，可以尝试强制推送：
     ```powershell
     git push -u origin master --force
     ```

3. **忽略文件**：
   - 确保 `.gitignore` 文件已配置，避免推送不必要的文件（如虚拟环境、缓存文件等）。

4. **冲突处理**：
   - 如果推送失败并提示冲突，需要先解决冲突后再提交和推送。

---

### **完整命令总结**
```powershell
cd /e:/self_test/agent_program/000-建模/000-通用
git init
git remote add origin https://github.com/zh2673-git/datadone.git
git branch -m main master  # 仅当本地分支为 main 时执行
git add .
git commit -m "Initial commit: project setup"
git push -u origin master
```

保存此流程，后续可直接使用！如有其他问题，随时联系。