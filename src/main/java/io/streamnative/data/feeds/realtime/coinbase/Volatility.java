package io.streamnative.data.feeds.realtime.coinbase;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Volatility {
    private long window_start_time;

    private long window_end_time;

    private String product_id;

    private float volatility;
}
