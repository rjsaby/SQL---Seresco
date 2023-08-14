DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOR table_name IN (select table_name 
  					from information_schema.tables 
  					WHERE table_schema='sanonofre_rural' AND table_type='BASE TABLE') 
  LOOP
    EXECUTE 'truncate table ' || tabla_name || ' cascade;';
  END LOOP;
END $$;

