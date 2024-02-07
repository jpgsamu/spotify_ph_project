SELECT STRFTIME('%Y-%m',ts) as month_ref

     , ROUND(SUM(1.0 * ms_played / 1000 / 60 / 24),1) as hours_played

     , SUM(1) as plays_qty
     , CAST(SUM(COALESCE(shuffle,0)) AS INT) as shuffle_qty
     , CAST(SUM(COALESCE(skipped,0)) AS INT) as skipped_qty 

     , SUM(CASE WHEN reason_start = 'trackdone' THEN 1 ELSE 0 END) AS start_trackdone_qty	
     , SUM(CASE WHEN reason_start = 'clickrow' THEN 1 ELSE 0 END) AS start_clickrow_qty	
     , SUM(CASE WHEN reason_start = 'fwdbtn' THEN 1 ELSE 0 END) AS start_fwdbtn_qty	
     , SUM(CASE WHEN reason_start = 'playbtn' THEN 1 ELSE 0 END) AS start_playbtn_qty	
     , SUM(CASE WHEN reason_start = 'backbtn' THEN 1 ELSE 0 END) AS start_backbtn_qty	
     , SUM(CASE WHEN reason_start = 'remote' THEN 1 ELSE 0 END) AS start_remote_qty	
     , SUM(CASE WHEN reason_start = 'appload' THEN 1 ELSE 0 END) AS start_appload_qty	
     , SUM(CASE WHEN reason_start = 'trackerror' THEN 1 ELSE 0 END) AS start_trackerror_qty	

     , SUM(CASE WHEN reason_end = 'trackdone' THEN 1 ELSE 0 END) AS end_trackdone_qty 
     , SUM(CASE WHEN reason_end = 'endplay' THEN 1 ELSE 0 END) AS end_endplay_qty 
     , SUM(CASE WHEN reason_end = 'fwdbtn' THEN 1 ELSE 0 END) AS end_fwdbtn_qty 
     , SUM(CASE WHEN reason_end = 'remote' THEN 1 ELSE 0 END) AS end_remote_qty 
     , SUM(CASE WHEN reason_end = 'logout' THEN 1 ELSE 0 END) AS end_logout_qty 
     , SUM(CASE WHEN reason_end = 'backbtn' THEN 1 ELSE 0 END) AS end_backbtn_qty 
     , SUM(CASE WHEN reason_end = 'paused' THEN 1 ELSE 0 END) AS end_paused_qty 
     , SUM(CASE WHEN reason_end = 'exit' THEN 1 ELSE 0 END) AS end_exit_qty 
     , SUM(CASE WHEN reason_end = 'unknown' THEN 1 ELSE 0 END) AS end_unknown_qty 
     , SUM(CASE WHEN reason_end = 'trackerror' THEN 1 ELSE 0 END) AS end_trackerror_qty 
     

FROM tb_spotify_ph

WHERE DATE(ts) >= '2022-10-15' -- "skip" started being triggered

GROUP BY 1
ORDER BY 1 
