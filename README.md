# 点对点群聊（P2P Group Chat）

无需注册、无需登录，打开即用。填写昵称即可加入群聊，同一房间内昵称唯一，不可重复。

## 特性

- 点对点（WebRTC）消息传输，消息直连用户之间，不经过任何服务器存储
- 无账号体系：只需输入昵称即可开始聊天
- 昵称唯一性：同一房间内不允许出现重复昵称，有人占用时会被拒绝
- 多房间支持：填写房间名即可创建/进入不同房间
- 在线成员列表、加入/离开提示
- 纯静态部署，可托管在 GitHub Pages 等任意静态托管

## 技术栈

- [Yjs](https://github.com/yjs/yjs) — 共享状态同步（CRDT）
- [y-webrtc](https://github.com/yjs/y-webrtc) — WebRTC 点对点传输（消息直连，仅信令服务器用于发现对方）
- [esbuild](https://esbuild.github.io/) — 打包构建

## 本地开发

```bash
npm install
npm run build   # 生成 bundle.js
```

构建后在任意静态服务器打开 `index.html` 即可（例如 `npx serve .`）。

## 在线访问

- https://lianchuzhong.github.io/ppe/
