-- Query to generate table of all CCDs
with images as (
  select distinct i.expnum, i.ccdnum, i.band, racmin, racmax, deccmin, deccmax, i.filename as ilink 
  from decade.image i, decade.file_archive_info fai 
  where i.filetype='red_immask' and i.filename=fai.filename 
  order by expnum, ccdnum
),  
catalogs as (
  select distinct c.expnum, c.ccdnum, c.filename as clink 
  from decade.catalog c, decade.file_archive_info fai 
  where c.filetype='cat_finalcut' and c.filename=fai.filename 
  order by expnum, ccdnum
), 
psfs as (
  select distinct m.expnum, m.ccdnum, m.filename as plink 
  from decade.miscfile m, decade.file_archive_info fai 
  where m.filetype='psfex_model' and m.filename=fai.filename 
  order by expnum, ccdnum
),
backgrounds as (
  select distinct i.expnum, i.ccdnum, i.filename as blink 
  from decade.image i, decade.file_archive_info fai 
  where i.filetype='red_bkg' and i.filename=fai.filename 
  order by expnum, ccdnum
),  
segmaps as (
  select distinct m.expnum, m.ccdnum, m.filename as slink 
  from decade.miscfile m, decade.file_archive_info fai 
  where m.filetype='red_segmap' and m.filename=fai.filename 
  order by expnum, ccdnum
)
select images.expnum as expnum, images.ccdnum as ccdnum, band, racmin, racmax, deccmin, deccmax, ilink, clink, plink, blink, slink 
from images, catalogs, psfs, backgrounds, segmaps 
where images.expnum = catalogs.expnum and images.ccdnum = catalogs.ccdnum and images.expnum = psfs.expnum and images.ccdnum = psfs.ccdnum and images.expnum = backgrounds.expnum and images.ccdnum = backgrounds.ccdnum and images.expnum = segmaps.expnum and images.ccdnum = segmaps.ccdnum;
> delve-images.csv
