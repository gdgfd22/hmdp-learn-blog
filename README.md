# 黑马点评学习项目

本仓库用于个人学习、实践与记录。

## 学习代码来源

本项目的基础学习代码来源于 [guojianwang/redis](https://gitee.com/guojianwang/redis)，并在学习过程中进行了功能扩展、实验验证与文档整理。

原项目及相关内容的权利归原作者所有。如需使用，请同时遵循原项目的许可与说明。

## React 前端

新前端位于 `frontend/`，使用 React 与 Vite 构建，包含：

- 城市生活发现首页：内容搜索、分类入口、故事筛选和数据看板入口；
- 实时运营分析页：核心指标、小时趋势、商户热度、优惠券漏斗和数据质量监控；
- 接口不可用时自动使用演示数据，方便独立预览界面。

```bash
cd frontend
npm install
npm run build
```

构建结果会输出到 `doc/nginx-1.18.0/html/hmdp/`，可由项目内现有 Nginx 直接托管。
