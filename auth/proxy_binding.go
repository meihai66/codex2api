package auth

// 自动绑定模式的代理-账号绑定管理（参考 kiro.rs/src/kiro/proxy_pool.rs）
//
// 设计要点：
//   - 一个代理有 slots（容量）+ expires_at（到期）+ 当前已绑账号数
//   - 添加账号时按 "空闲槽位多 → 到期最远" 策略自动挑一个代理
//   - 失败的代理 5 分钟内不再被自动选中（recent_failure cooldown）
//   - 绑定关系存独立表 account_proxy_bindings，与 accounts.proxy_url 字段并存
//     （后者用于"手动覆盖"——最高优先级，永远先用）

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/codex2api/database"
)

// recentFailureCooldown 失败冷却时长：被标记后 5 分钟内不会被自动 pick / auto-bind 选中
const recentFailureCooldown = 5 * time.Minute

// minBindValidityHours 自动绑定时要求代理至少还有这么多小时有效期；
// 0 表示没有 expires_at 的代理（永不过期，迁移过来的老数据）也可被选中
const minBindValidityHours = 1

// BindingManager 代理绑定管理器
type BindingManager struct {
	db *database.DB

	mu             sync.RWMutex
	recentFailures map[int64]time.Time // proxyID → 失败时刻

	// bindMu 串行化 AutoBind 的"读快照 → 挑代理 → 写绑定"流程，
	// 避免并发导入时多个 goroutine 看到同一份 used_slots 快照而把同一代理绑爆
	bindMu sync.Mutex
}

// NewBindingManager 创建管理器
func NewBindingManager(db *database.DB) *BindingManager {
	return &BindingManager{
		db:             db,
		recentFailures: make(map[int64]time.Time),
	}
}

// MarkRecentFailure 标记某代理"最近失败"
func (m *BindingManager) MarkRecentFailure(proxyID int64) {
	if m == nil || proxyID <= 0 {
		return
	}
	m.mu.Lock()
	m.recentFailures[proxyID] = time.Now()
	m.mu.Unlock()
}

// ClearRecentFailure 清除标记（验证成功后可选调用）
func (m *BindingManager) ClearRecentFailure(proxyID int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	delete(m.recentFailures, proxyID)
	m.mu.Unlock()
}

// isInFailureCooldown 是否还在冷却内
func (m *BindingManager) isInFailureCooldown(proxyID int64) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	t, ok := m.recentFailures[proxyID]
	m.mu.RUnlock()
	return ok && time.Since(t) < recentFailureCooldown
}

// FindBindingFor 查找账号当前绑定的代理 id；未绑定返回 (0, nil)
func (m *BindingManager) FindBindingFor(ctx context.Context, accountID int64) (int64, error) {
	if m == nil || m.db == nil {
		return 0, nil
	}
	return m.db.FindProxyBindingForAccount(ctx, accountID)
}

// FindProxyByID 取代理详情
func (m *BindingManager) FindProxyByID(ctx context.Context, proxyID int64) (*database.ProxyRow, error) {
	if m == nil || m.db == nil {
		return nil, errors.New("binding manager not initialized")
	}
	rows, err := m.db.ListEnabledProxies(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range rows {
		if p.ID == proxyID {
			return p, nil
		}
	}
	return nil, nil
}

// AutoBind 为账号挑一个最优代理并写入绑定
//
// 选择策略（按 kiro.rs）：
//  1. 仅考虑 enabled = true 的代理
//  2. 仍有剩余槽位（used < slots）
//  3. 剩余有效期 > minBindValidityHours（or expires_at IS NULL）
//  4. 不在 recent_failure 冷却内
//  5. 排序：剩余槽位最多 > 到期最远（永不过期视为无穷远）
//
// 返回 (proxyID, nil) 表示绑定成功；(0, err) 表示无可用代理。
func (m *BindingManager) AutoBind(ctx context.Context, accountID int64) (int64, error) {
	if m == nil || m.db == nil || accountID <= 0 {
		return 0, errors.New("invalid binding manager / account id")
	}
	m.bindMu.Lock()
	defer m.bindMu.Unlock()
	proxies, err := m.db.ListEnabledProxies(ctx)
	if err != nil {
		return 0, err
	}
	now := time.Now()
	var best *database.ProxyRow
	for _, p := range proxies {
		used := p.UsedSlots
		avail := p.Slots - used
		if avail <= 0 {
			continue
		}
		if p.ExpiresAt != nil {
			if p.ExpiresAt.Sub(now) < time.Duration(minBindValidityHours)*time.Hour {
				continue
			}
		}
		if m.isInFailureCooldown(p.ID) {
			continue
		}
		if best == nil || isBetterCandidate(p, best) {
			best = p
		}
	}
	if best == nil {
		if len(proxies) == 0 {
			return 0, errors.New("代理池为空：请先添加至少一个已启用代理")
		}
		return 0, errors.New("代理池槽位已满 / 有效期不足：所有已启用代理均无可用槽位")
	}
	if err := m.db.UpsertProxyBinding(ctx, accountID, best.ID); err != nil {
		return 0, err
	}
	return best.ID, nil
}

// isBetterCandidate a 是否比 b 更优：剩余槽位多优先；相同则到期更远优先（nil 视为 +∞）
func isBetterCandidate(a, b *database.ProxyRow) bool {
	aAvail := a.Slots - a.UsedSlots
	bAvail := b.Slots - b.UsedSlots
	if aAvail != bAvail {
		return aAvail > bAvail
	}
	// 都不过期，谁先谁说了算（保持稳定）
	if a.ExpiresAt == nil && b.ExpiresAt == nil {
		return false
	}
	if a.ExpiresAt == nil {
		return true
	}
	if b.ExpiresAt == nil {
		return false
	}
	return a.ExpiresAt.After(*b.ExpiresAt)
}

// ManualBind 手动指定绑定（覆盖现有绑定）；不校验槽位是否满，由调用方自行决定
func (m *BindingManager) ManualBind(ctx context.Context, accountID, proxyID int64) error {
	if m == nil || m.db == nil {
		return errors.New("binding manager not initialized")
	}
	return m.db.UpsertProxyBinding(ctx, accountID, proxyID)
}

// Unbind 解除指定账号的绑定
func (m *BindingManager) Unbind(ctx context.Context, accountID int64) error {
	if m == nil || m.db == nil {
		return nil
	}
	return m.db.DeleteProxyBindingByAccount(ctx, accountID)
}

// ResolveProxyURL 根据绑定关系解析出账号该用的代理 URL；未绑定/已禁用/已过期返回 ""
func (m *BindingManager) ResolveProxyURL(ctx context.Context, accountID int64) (string, error) {
	if m == nil || m.db == nil || accountID <= 0 {
		return "", nil
	}
	proxyID, err := m.db.FindProxyBindingForAccount(ctx, accountID)
	if err != nil {
		return "", err
	}
	if proxyID == 0 {
		return "", nil
	}
	proxies, err := m.db.ListEnabledProxies(ctx)
	if err != nil {
		return "", err
	}
	now := time.Now()
	for _, p := range proxies {
		if p.ID != proxyID {
			continue
		}
		if p.ExpiresAt != nil && !p.ExpiresAt.After(now) {
			return "", nil // 已过期
		}
		return p.URL, nil
	}
	return "", nil // 已禁用或被删
}
