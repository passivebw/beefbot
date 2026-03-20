"""volume.py - Volume spike detection."""
from __future__ import annotations
from dataclasses import dataclass
from app.data.market_state import MarketStateSnapshot


@dataclass
class VolumeFeatures:
    volume_1m_btc: float
    volume_baseline_btc: float
    spike_ratio: float
    is_spike: bool
    spike_strength: float
    is_valid: bool


def compute_volume_spike(snapshot: MarketStateSnapshot,
                          spike_multiplier: float = 2.5,
                          current_window_s: float = 60.0,
                          baseline_window_s: float = 300.0) -> VolumeFeatures:
    trades = snapshot.trade_tape
    now = snapshot.now
    if not trades:
        return VolumeFeatures(0.0, 0.0, 1.0, False, 0.0, False)

    vc = sum(t.qty for t in trades if t.timestamp >= now - current_window_s)
    vb_total = sum(t.qty for t in trades if t.timestamp >= now - baseline_window_s)
    vb = vb_total / (baseline_window_s / current_window_s)

    if vb < 1e-9:
        return VolumeFeatures(vc, vb, 1.0, False, 0.0, True)

    ratio = vc / vb
    is_spike = ratio >= spike_multiplier
    strength = min(1.0, max(0.0, (ratio - 1.0) / (spike_multiplier + 1.0)))
    return VolumeFeatures(vc, vb, ratio, is_spike, strength, True)
