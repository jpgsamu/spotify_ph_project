
-- DROP TABLE tb_abt_spotify_ph 
CREATE TABLE tb_abt_spotify_ph as
with song_log as (

SELECT  ts

      , CASE WHEN STRFTIME('%w',ts) = '0' THEN 'sun'
             WHEN STRFTIME('%w',ts) = '1' THEN 'mon'
             WHEN STRFTIME('%w',ts) = '2' THEN 'tue'
             WHEN STRFTIME('%w',ts) = '3' THEN 'wed'
             WHEN STRFTIME('%w',ts) = '4' THEN 'thu'
             WHEN STRFTIME('%w',ts) = '5' THEN 'fri'
             WHEN STRFTIME('%w',ts) = '6' THEN 'sat' END as dow

      , CASE WHEN CAST(STRFTIME('%H',ts) AS INT) < 6  THEN 'latenight' 
             WHEN CAST(STRFTIME('%H',ts) AS INT) < 13 THEN 'morning'
             WHEN CAST(STRFTIME('%H',ts) AS INT) < 19 THEN 'afternoon'
             ELSE 'earlynight' END as tod        

      , master_metadata_track_name	as track
      , master_metadata_album_artist_name as artist
      , master_metadata_album_album_name as album
      
      , ms_played
      , skipped

      , CASE WHEN reason_start = 'trackdone' THEN 1 ELSE 0 END AS start_trackdone	
      , CASE WHEN reason_start = 'clickrow' THEN 1 ELSE 0 END AS start_clickrow	
      , CASE WHEN reason_start = 'fwdbtn' THEN 1 ELSE 0 END AS start_fwdbtn	
      , CASE WHEN reason_start = 'playbtn' THEN 1 ELSE 0 END AS start_playbtn	
      , CASE WHEN reason_start = 'backbtn' THEN 1 ELSE 0 END AS start_backbtn	
      , CASE WHEN reason_start = 'remote' THEN 1 ELSE 0 END AS start_remote	
      , CASE WHEN reason_start = 'appload' THEN 1 ELSE 0 END AS start_appload	
      , CASE WHEN reason_start = 'trackerror' THEN 1 ELSE 0 END AS start_trackerror_qty

      , CASE WHEN reason_end = 'trackdone' THEN 1 ELSE 0 END AS end_trackdone 
      , CASE WHEN reason_end = 'endplay' THEN 1 ELSE 0 END AS end_endplay 
      , CASE WHEN reason_end = 'fwdbtn' THEN 1 ELSE 0 END AS end_fwdbtn 
      , CASE WHEN reason_end = 'remote' THEN 1 ELSE 0 END AS end_remote 
      , CASE WHEN reason_end = 'logout' THEN 1 ELSE 0 END AS end_logout 
      , CASE WHEN reason_end = 'backbtn' THEN 1 ELSE 0 END AS end_backbtn 
      , CASE WHEN reason_end = 'paused' THEN 1 ELSE 0 END AS end_paused 
      , CASE WHEN reason_end = 'exit' THEN 1 ELSE 0 END AS end_exit 
      , CASE WHEN reason_end = 'unknown' THEN 1 ELSE 0 END AS end_unknown 
      , CASE WHEN reason_end = 'trackerror' THEN 1 ELSE 0 END AS end_trackerror 

FROM tb_spotify_ph
 
WHERE DATE(ts) >= '2022-10-15' )

, extended_log as (
SELECT  *
      , ROW_NUMBER () OVER (PARTITION BY track, artist, album
                         ORDER BY ts) - 1 as plays_acc

      , COALESCE(SUM(ms_played) OVER (PARTITION BY track, artist, album
                                ORDER BY ts
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as ms_acc 
      
      , COALESCE(AVG(ms_played) OVER (PARTITION BY track, artist, album
                                ORDER BY ts
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as ms_avg

      , COALESCE(SUM(skipped)   OVER (PARTITION BY track, artist, album
                                ORDER BY ts
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as skipped_acc 
      
      , COALESCE(SUM(start_trackdone) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as start_trackdone_acc
      , COALESCE(SUM(start_clickrow) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as start_clickrow_acc
      , COALESCE(SUM(start_fwdbtn) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as	start_fwdbtn_acc
      , COALESCE(SUM(start_playbtn) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as	start_playbtn_acc
      , COALESCE(SUM(start_backbtn) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as	start_backbtn_acc
      , COALESCE(SUM(start_remote) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as	start_remote_acc
      , COALESCE(SUM(start_appload) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as	start_appload_acc
      , COALESCE(SUM(start_trackerror_qty) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) start_trackerror_qty_acc
      
      , COALESCE(SUM(end_trackdone) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as end_trackdone_acc
      , COALESCE(SUM(end_endplay) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as end_endplay_acc
      , COALESCE(SUM(end_fwdbtn) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as end_fwdbtn_acc
      , COALESCE(SUM(end_remote) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as end_remote_acc
      , COALESCE(SUM(end_logout) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as end_logout_acc
      , COALESCE(SUM(end_backbtn) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as end_backbtn_acc
      , COALESCE(SUM(end_paused) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as end_paused_acc
      , COALESCE(SUM(end_exit) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as end_exit_acc
      , COALESCE(SUM(end_unknown) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as end_unknown_acc
      , COALESCE(SUM(end_trackerror) OVER (PARTITION BY track, artist, album ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) as end_trackerror_acc
      
FROM song_log)
--

, clean_log as (
SELECT  ts
      , track
      , artist
      , album

      , dow
      , tod

      , plays_acc
      , ms_acc
      , ms_avg
           
      , 1.0 * COALESCE(skipped_acc / plays_acc, -1) as perc_skipped_acc 

      , COALESCE(1.0 * start_trackdone_acc / plays_acc, -1) as perc_start_trackdone_acc
      , COALESCE(1.0 * start_clickrow_acc / plays_acc, -1) as perc_start_clickrow_acc
      , COALESCE(1.0 * start_fwdbtn_acc / plays_acc, -1) as perc_start_fwdbtn_acc
      , COALESCE(1.0 * start_playbtn_acc / plays_acc, -1) as perc_start_playbtn_acc
      , COALESCE(1.0 * start_backbtn_acc / plays_acc, -1) as perc_start_backbtn_acc
      , COALESCE(1.0 * start_remote_acc / plays_acc, -1) as perc_start_remote_acc
      , COALESCE(1.0 * start_appload_acc / plays_acc, -1) as perc_start_appload_acc
      , COALESCE(1.0 * start_trackerror_qty_acc / plays_acc, -1) as perc_start_trackerror_qty_acc   
      
      , COALESCE(1.0 * end_trackdone_acc / plays_acc, -1) as perc_end_trackdone_acc
      , COALESCE(1.0 * end_endplay_acc / plays_acc, -1) as perc_end_endplay_acc
      , COALESCE(1.0 * end_fwdbtn_acc / plays_acc, -1) as perc_end_fwdbtn_acc
      , COALESCE(1.0 * end_remote_acc / plays_acc, -1) as perc_end_remote_acc
      , COALESCE(1.0 * end_logout_acc / plays_acc, -1) as perc_end_logout_acc
      , COALESCE(1.0 * end_backbtn_acc / plays_acc, -1) as perc_end_backbtn_acc
      , COALESCE(1.0 * end_paused_acc / plays_acc, -1) as perc_end_paused_acc
      , COALESCE(1.0 * end_exit_acc / plays_acc, -1) as perc_end_exit_acc
      , COALESCE(1.0 * end_unknown_acc / plays_acc, -1) as perc_end_unknown_acc
      , COALESCE(1.0 * end_trackerror_acc / plays_acc, -1) as perc_end_trackerror_acc

      , CAST (skipped AS INT) as skipped

FROM extended_log

ORDER BY ts)
--

SELECT * FROM clean_log;