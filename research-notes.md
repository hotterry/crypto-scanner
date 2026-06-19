# 策略研究笔记

## 研究方法
通过深度研究 (deep-research workflow) 综合搜索 X/Twitter、Reddit、BitcoinTalk、TradingView 等海外论坛，结合 arXiv、SSRN、Springer 等学术来源，交叉验证 25 条核心声明，最终确认 8 条、否定 17 条。

## 关键发现

### 验证通过的策略

#### 1. 布林带均值回归 (小时线)
- 来源: Theseus 论文 (https://www.theseus.fi/handle/10024/902903)
- BTC 在 1 小时级别呈均值回归特性
- 年化收益 74.8% (净), 夏普比率 1.48
- 关键发现: **时间框架决定策略有效性** — 日线买入持有(678%)碾压均值回归(-9.67%), 但小时线均值回归(106.8%年化)反超
- 包含 0.1% 手续费后仍显著优于动量和买入持有

#### 2. 动态网格交易 (DGT)
- 来源: arXiv 2506.11921 (NTU 团队)
- 数学证明: 固定边界几何网格在随机游走中期望值为零
- DGT 变体: 价格突破边界时动态重置网格，转终止游戏为自维持游戏
- 报告 BTC 回测 60-70% 年化 IRR

#### 3. RBF 假突破
- 来源: IndieHackers 回测 (https://www.indiehackers.com/post/backtested-13-strategies-for-extreme-fear-markets-11-failed-heres-what-worked-8952d6948b)
- 极度恐惧市场中 13 种策略仅 2 种盈利
- RBF (Range Break Failure) 在 238 次交易中 56% 胜率

### 被否定的策略

| 策略 | 验证票数 | 原因 |
|------|---------|------|
| DQN 强化学习择时 | 0-3 | 宣称 120x NAV 增长, 无法独立复现 |
| Copula 配对交易 | 0-3 | 宣称 205.9% 净回报, 不可复现 |
| CARVS 情绪策略 | 0-3 | 宣称跑赢 4150%, 来源可靠性低 |
| 波动率调整动量 (日线) | 1-2 | 未跑赢简单买入持有 |

### 核心警示
- **没有任何策略被独立多方验证能在多个市场周期持续盈利**
- 7 策略 24 个月实盘回测: **全部亏损**, 0 个突破 +36% 门槛
- 胜率是陷阱: 93.3% 胜率却 -95.48% 总回报 (567 次小盈利被 39 次止损抹杀)
- 手续费/资金费率/滑点是隐性杀手

## 参考文献
1. arXiv 2506.11921 - Dynamic Grid Trading (NTU)
2. Theseus 论文 - Bitcoin Mean Reversion vs Momentum
3. GitHub francisx1999 - Crypto Trading Bot Postmortem
4. IndieHackers - 13 Strategies for Extreme Fear Markets
5. Springer - Copula-based Pairs Trading in Crypto
6. SSRN - CARVS Cryptocurrency Algorithm
7. Taylor & Francis - DQN RL for Crypto Trading
8. CEUR-WS - Crypto Arbitrage Bot
