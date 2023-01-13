select name_track,name_album,name_artist,duration_ms
from track as t
left join album as a on a.id = t.id_album
left join artist as ar on ar.id = a.id_artist;

select * from artist;
select * from album;
select * from track;

delete from track where name_track = 'Continuar';

select name_track from track where name_track = 'Continuar';

select name_album from album where name_album = 'Histórias e Bicicletas';

delete from album where name_album = 'Histórias e Bicicletas';

select name_album from album where name_album = 'Histórias e Bicicletas';

select name_track from track;

select name_artist from artist where name_artist = 'Oficina G3';

delete from artist where name_artist = 'Oficina G3';

select name_artist from artist where name_artist = 'Oficina G3';

select name_album from album;

select name_track from track;










