-- Query to generate table of all CCDs

with images as (
     select distinct i.expnum, i.ccdnum, i.band, racmin, racmax, deccmin, deccmax, 'http://decade.ncsa.illinois.edu/deca_archive/'||fai.path||'/'||i.filename||fai.compression as ilink from decade.proctag t, decade.image i, decade.file_archive_info fai where t.pfw_attempt_id=i.pfw_attempt_id and i.filetype='red_immask' and i.filename=fai.filename order by expnum, ccdnum),
     catalogs as (select distinct c.expnum, c.ccdnum, 'http://decade.ncsa.illinois.edu/deca_archive/'||fai.path||'/'||c.filename||fai.compression as clink from decade.proctag t, decade.catalog c, decade.file_archive_info fai where t.pfw_attempt_id=c.pfw_attempt_id and c.filetype='cat_finalcut' and c.filename=fai.filename order by expnum, ccdnum)
     select images.expnum as expnum, images.ccdnum as ccdnum, band, racmin, racmax, deccmin, deccmax, ilink, clink
     from images, catalogs where images.expnum = catalogs.expnum and images.ccdnum = catalogs.ccdnum;
> delve-images.csv
