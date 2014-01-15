CREATE PROCEDURE bang(IN n INT, OUT s VARCHAR(1024))
  LANGUAGE javascript PARAMETER STYLE variables AS $$
  s = "";
  while (n-- > 0) s += "!";
$$;

CALL bang(5);
