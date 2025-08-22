from dataclasses import dataclass
from typing import Optional, Dict

def _norm(x: Optional[float], lo: float, hi: float, invert: bool = False) -> float:
    if x is None:
        return 0.0
    if hi == lo:
        return 0.0
    v = (x - lo) / (hi - lo)
    v = max(0.0, min(1.0, v))
    return 1.0 - v if invert else v

@dataclass
class Row:
    mint: str
    symbol: Optional[str]
    decimals: int
    mint_authority_null: int
    freeze_authority_null: int
    token2022_danger: int
    lp_verified: int
    tvl_usd: Optional[float]
    impact_1k_pct: Optional[float]
    spread_pct: Optional[float]
    r_1m: float
    r_5m: float
    r_15m: float
    volume_5m: float
    unique_buyers_5m: int
    net_buy_usd_5m: float
    top10_pct: Optional[float]

def score_row(r: Row) -> Dict[str, float]:
    # Safety
    s_mint = 1.0 if r.mint_authority_null == 1 else 0.0
    s_freeze = 1.0 if r.freeze_authority_null == 1 else 0.0
    s_t22 = 1.0 if r.token2022_danger == 0 else 0.0
    s_lp = 1.0 if r.lp_verified == 1 else 0.0
    dec_bonus = 1.0 if 0 <= r.decimals <= 12 else 0.5
    safety = (0.25 * s_mint + 0.25 * s_freeze + 0.25 * s_t22 + 0.25 * s_lp) * dec_bonus

    # Ликвидность/рынок
    liq = _norm(r.tvl_usd, 1e4, 1e6)
    impact = _norm(abs(r.impact_1k_pct) if r.impact_1k_pct is not None else None, 0.0, 5.0, invert=True)
    spread = _norm(r.spread_pct, 0.0, 1.0, invert=True)
    market_quality = 0.5 * impact + 0.5 * spread

    # Импульс
    mom_5m = _norm(r.r_5m, 0.0, 0.05)
    mom_15m = _norm(r.r_15m, 0.0, 0.08)
    momentum = 0.6 * mom_5m + 0.4 * mom_15m

    # Потоки
    flows = 0.6 * _norm(r.net_buy_usd_5m, 0.0, 100_000.0) + 0.4 * _norm(float(r.unique_buyers_5m), 0.0, 300.0)

    # Дистрибуция
    distr = _norm(r.top10_pct, 0.2, 0.9, invert=True)

    score = (
        0.35 * safety +
        0.20 * liq +
        0.15 * market_quality +
        0.15 * momentum +
        0.10 * flows +
        0.05 * distr
    )
    return {
        "score": max(0.0, min(1.0, score)),
        "safety": safety,
        "liq": liq,
        "mkt": market_quality,
        "momentum": momentum,
        "flows": flows,
        "distr": distr,
    }
