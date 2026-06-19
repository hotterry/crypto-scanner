# 合约交易策略终端

## 概述
基于深度学习研究验证的加密货币合约交易策略终端。集成 Binance/OKX/Bybit 实时 WebSocket 行情、6种策略信号引擎、智能止盈止损计算、回测系统和交易所 API 接入。

## 核心功能

### 📡 实时行情
- Binance WebSocket 实时 K 线推送 (1h + 4h 双时间框架)
- 支持 OKX、Bybit WebSocket（自动切换）
- Binance REST API 历史 K 线拉取
- 断线自动重连，失败降级模拟数据

### 🎯 策略引擎 (基于学术研究验证)
| 策略 | 时间框架 | 核心逻辑 | 研究依据 |
|------|---------|---------|---------|
| 布林带均值回归 | 1h | 价触下轨做多 / 上轨做空 | 夏普 1.48, 年化 74.8% |
| RSI 超买超卖 | 1h | RSI<30 做多 / >70 做空 + EMA50 过滤 | 经典指标验证 |
| MACD 金叉死叉 | 4h | 柱状图确认 + 零轴过滤 | 趋势行情高效 |
| EMA 趋势跟踪 | 4h | EMA50/200 金叉死叉 + 回调入场 | 中线趋势核心 |
| RBF 假突破 | 1h | 区间假突破后反向回归 | 238笔交易 56%胜率 |
| 综合多策略 | 1h | 布林+RSI+MACD ≥2共振 | 降低假信号率 |

### 🛑 智能 SL/TP 引擎
- 止损: ATR(14)×1.5 / 硬止损2% / 20K结构支撑阻力 取最紧
- 止盈: TP1(1:1.5) / TP2(1:2.5) / TP3(布林结构目标)
- 仓位: (账户余额 × 风险%) / (入场价 - 止损) × 杠杆
- 每产生交易信号自动附上完整 SL/TP 建议

### 🔔 信号提醒
- 页面内浮动弹窗卡片 (8秒自动消失)
- 浏览器原生 Notification API 通知
- 提示音 (Web Audio API)
- 信号去重，避免重复提醒

### 🧪 回测引擎
- 6策略 × 4时间框架 × 5币种 × 可调杠杆/手续费/风险/止损
- 输出: 总收益、胜率、盈亏比、最大回撤、权益曲线图、逐笔明细

### 🔌 交易所 API
- Binance Futures / OKX / Bybit
- API Key 本地 localStorage 加密存储
- 自动交易开关 (策略信号 → 市价单)

## 快速启动

```bash
# 1. 进入项目目录
cd contract-terminal-project

# 2. 启动本地 HTTP 服务器
python3 -m http.server 8765

# 3. 浏览器打开
open http://localhost:8765/contract-terminal.html
```

或使用 Claude Code 预览:
```bash
claude preview start contract-terminal
```

## 文件结构

```
contract-terminal-project/
├── contract-terminal.html    # 主应用 (单文件, 包含 HTML/CSS/JS)
├── launch.json               # Claude Code 预览配置
├── README.md                 # 本文件
└── research-notes.md         # 策略研究笔记
```

## 技术栈
- 纯前端: HTML5 + CSS3 + Vanilla JavaScript (零依赖)
- WebSocket: Binance/OKX/Bybit 实时数据流
- Canvas: 价格图表 & 权益曲线绘制
- Web Audio API: 信号提示音
- Notification API: 浏览器桌面通知
- localStorage: 策略/API 配置持久化

## 浏览器兼容
- Chrome 90+ / Edge 90+ / Safari 15+ / Firefox 90+
- 需要允许通知权限以获得信号弹窗提醒

## 免责声明
⚠️ 本工具仅供研究和教育目的。加密货币交易风险极高，过往回测表现不保证未来收益。使用前请充分理解策略逻辑并自行承担风险。
