with t1 as (

SELECT  track
      , artist
      , album
      , perc_skipped_acc
      , plays_acc	
      , ROW_NUMBER () OVER (PARTITION BY track, artist, album ORDER BY ts DESC) as rown

FROM tb_abt_spotify_ph

WHERE plays_acc > 10)
--

SELECT  track
      , artist 
      , plays_acc
      , 1 - perc_skipped_acc AS perc_not_skipped

FROM t1

WHERE rown = 1

ORDER BY perc_skipped_acc ASC

