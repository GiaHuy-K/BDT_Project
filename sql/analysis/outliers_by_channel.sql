-- Outliers: one threshold for every channel vs one threshold per channel
SELECT "Canal_ID", rows, channel_threshold, flagged_global, flagged_global_pct, flagged_channel, flagged_channel_pct, newly_flagged
FROM gold.outliers_by_channel
ORDER BY rows DESC;
