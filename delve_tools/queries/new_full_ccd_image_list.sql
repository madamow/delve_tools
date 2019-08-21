-- Query to generate table of all CCD file paths
-- paths should be appended to 'http://decade.ncsa.illinois.edu/deca_archive/'
select i.expnum, i.ccdnum, i.band, i.racmin, i.racmax, i.deccmin, i.deccmax,
       i.rac1, i.rac2, i.rac3, i.rac4,
       i.decc1, i.decc2, i.decc3, i.decc4,
       fai_i.path||'/'||i.filename||fai_i.compression as ilink,
       fai_c.path||'/'||c.filename||fai_c.compression as clink,
       fai_p.path||'/'||m.filename||fai_p.compression as plink,
       fai_b.path||'/'||b.filename||fai_b.compression as blink
from decade.image i, decade.catalog c, decade.miscfile m, decade.image b,
     decade.file_archive_info fai_i,
     decade.file_archive_info fai_c,
     decade.file_archive_info fai_p,
     decade.file_archive_info fai_b
where i.expnum = c.expnum and i.ccdnum = c.ccdnum
      and i.expnum = m.expnum and i.ccdnum = m.ccdnum
      and i.expnum = b.expnum and i.ccdnum = b.ccdnum
      and i.filetype='red_immask'   and i.filename=fai_i.filename
      and c.filetype='cat_finalcut' and c.filename=fai_c.filename
      and m.filetype='psfex_model'  and m.filename=fai_p.filename
      and b.filetype='red_bkg'      and b.filename=fai_b.filename
      --and rownum < 10000
      order by expnum, ccdnum;
> new_full_ccd_image_list.csv
