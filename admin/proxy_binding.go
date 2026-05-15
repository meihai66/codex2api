package admin

import (
	"context"
	"log"
	"strings"
)

// tryAutoBindProxy 在账号添加成功后尝试自动绑定一个代理。
//
// 规则：
//   - 如果调用方在 manualProxyURL 已显式指定了"账号级专属代理"（accounts.proxy_url 字段），
//     直接返回 nil，跳过自动绑定——手动指定的优先级最高，ResolveProxyForAccount 永远先用它。
//   - 否则调 store.AutoBindAccount 走 kiro.rs 同款"槽位多 + 到期远"策略挑代理。
//   - 失败（无可用代理 / 代理池为空 / 槽位全满）返回 error，由调用方决定是否回滚账号；
//     这是为了保证"不允许没有绑定 ip 就直连出现"——一旦无法绑定就不让该账号留下来跑请求。
func (h *Handler) tryAutoBindProxy(ctx context.Context, accountID int64, manualProxyURL string) error {
	if h == nil || h.store == nil || accountID <= 0 {
		return nil
	}
	if strings.TrimSpace(manualProxyURL) != "" {
		return nil
	}
	proxyID, err := h.store.AutoBindAccount(ctx, accountID)
	if err != nil {
		log.Printf("自动绑定代理失败 account=%d: %v", accountID, err)
		return err
	}
	if proxyID > 0 {
		log.Printf("已自动绑定代理 account=%d → proxy=%d", accountID, proxyID)
	}
	return nil
}
