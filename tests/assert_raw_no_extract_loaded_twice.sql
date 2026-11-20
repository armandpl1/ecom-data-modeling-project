-- The raw layer is append-only, so loading the same extract twice would
-- duplicate its rows. scripts/load_raw.py skips a file whose checksum is
-- already logged; this test proves it: no table has the same file twice.

select
    table_name,
    sha256,
    count(*) as times_loaded
from {{ source('raw', '_load_log') }}
group by table_name, sha256
having count(*) > 1
