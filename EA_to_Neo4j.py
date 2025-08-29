# This script converts an EA (Enterprise Architect) model SQL file into a Neo4j Cypher script.
# python EA_to_Neo4j.py EA_Model.sql output.cypher
# cat output.cypher | cypher-shell -u neo4j -p <password>
# CALL db.labels() YIELD label RETURN label, count(*)；

# docker exec unruffled_mcnulty cypher-shell -u neo4j -p "12345678" < qeaxastext.cypher


import re
import sys

def parse_create_table_defs(sql_text):
    tables = {}
    i = 0
    n = len(sql_text)
    while i < n:
        idx = sql_text.find("CREATE TABLE", i)
        if idx == -1:
            break
        start_idx = idx + len("CREATE TABLE")
        while start_idx < n and sql_text[start_idx].isspace():
            start_idx += 1
        end_idx = start_idx
        while end_idx < n and sql_text[end_idx] not in " \t\n(":
            end_idx += 1
        table_name = sql_text[start_idx:end_idx]
        # Find the opening parenthesis for column definitions
        while end_idx < n and sql_text[end_idx] != '(':
            end_idx += 1
        if end_idx >= n or sql_text[end_idx] != '(':
            i = end_idx
            continue
        depth = 0
        cols_start = end_idx + 1
        j = cols_start
        found_end = False
        while j < n:
            ch = sql_text[j]
            if ch == '(':
                depth += 1
            elif ch == ')':
                if depth == 0:
                    found_end = True
                    break
                else:
                    depth -= 1
            j += 1
        if not found_end:
            i = end_idx + 1
            continue
        cols_end = j
        cols_str = sql_text[cols_start:cols_end]
        # Split column definitions by commas at top level
        col_names = []
        col_buf = ""
        paren = 0
        in_quote = False
        k = 0
        while k < len(cols_str):
            ch = cols_str[k]
            if ch == "'" and not in_quote:
                in_quote = True
            elif ch == "'" and in_quote:
                # Handle escaped quotes in identifiers (e.g. 'Constraint')
                if k+1 < len(cols_str) and cols_str[k+1] == "'":
                    col_buf += "'"
                    k += 1
                else:
                    in_quote = False
            if not in_quote and ch == '(':
                paren += 1
            elif not in_quote and ch == ')':
                if paren > 0:
                    paren -= 1
            elif not in_quote and ch == ',' and paren == 0:
                # End of a column definition
                col_name_line = col_buf.strip()
                if col_name_line:
                    # Extract column name (handle quoted names)
                    if col_name_line[0] in "'\"`":
                        quote_char = col_name_line[0]
                        endq = col_name_line.find(quote_char, 1)
                        col_name = col_name_line[1:endq] if endq != -1 else col_name_line[1:]
                    else:
                        match = re.match(r"[^ \t]+", col_name_line)
                        col_name = match.group(0) if match else col_name_line
                    col_name = col_name.rstrip(",")
                    col_names.append(col_name)
                col_buf = ""
                # Skip any whitespace after the comma
                k += 1
                while k < len(cols_str) and cols_str[k].isspace():
                    k += 1
                continue
            else:
                col_buf += ch
            k += 1
        # Last column after loop
        col_name_line = col_buf.strip()
        if col_name_line:
            if col_name_line[0] in "'\"`":
                quote_char = col_name_line[0]
                endq = col_name_line.find(quote_char, 1)
                col_name = col_name_line[1:endq] if endq != -1 else col_name_line[1:]
            else:
                match = re.match(r"[^ \t]+", col_name_line)
                col_name = match.group(0) if match else col_name_line
            col_name = col_name.rstrip(",")
            col_names.append(col_name)
        tables[table_name] = col_names
        i = cols_end + 1
    return tables

def parse_inserts(sql_text):
    inserts = {}
    idx = 0
    n = len(sql_text)
    while idx < n:
        pos = sql_text.find("INSERT INTO", idx)
        if pos == -1:
            break
        start = pos + len("INSERT INTO")
        while start < n and sql_text[start].isspace():
            start += 1
        end = start
        while end < n and sql_text[end] not in " \t\n":
            end += 1
        table_name = sql_text[start:end]
        val_pos = sql_text.find("VALUES", end)
        if val_pos == -1:
            idx = end
            continue
        # Find the start of the VALUES tuple
        val_start = sql_text.find("(", val_pos)
        if val_start == -1:
            idx = val_pos + 1
            continue
        # Find the terminating semicolon of this INSERT (taking quotes into account)
        scan = val_start
        depth = 0
        in_quote = False
        statement_end = None
        while scan < n:
            ch = sql_text[scan]
            if ch == "'" and not in_quote:
                in_quote = True
            elif ch == "'" and in_quote:
                if scan+1 < n and sql_text[scan+1] == "'":
                    scan += 1  # skip escaped quote ('' -> ')
                else:
                    in_quote = False
            if not in_quote:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    if depth > 0:
                        depth -= 1
                elif ch == ';' and depth == 0:
                    statement_end = scan
                    break
            scan += 1
        if statement_end is None:
            break
        stmt = sql_text[pos:statement_end]
        # Extract the values inside the outer parentheses
        open_paren = stmt.find("(", stmt.find("VALUES"))
        close_paren = stmt.rfind(")")
        if open_paren == -1 or close_paren == -1:
            idx = statement_end + 1
            continue
        values_str = stmt[open_paren+1:close_paren]
        # Split values by commas at top level
        values = []
        buf = ""
        paren_count = 0
        in_q = False
        k = 0
        while k < len(values_str):
            ch = values_str[k]
            if ch == "'" and not in_q:
                in_q = True
                buf += ch
            elif ch == "'" and in_q:
                if k+1 < len(values_str) and values_str[k+1] == "'":
                    buf += "''"  # escaped quote stays as two single quotes
                    k += 1
                else:
                    in_q = False
                    buf += ch
            elif not in_q:
                if ch == '(':
                    paren_count += 1
                    buf += ch
                elif ch == ')':
                    if paren_count > 0:
                        paren_count -= 1
                    buf += ch
                elif ch == ',' and paren_count == 0:
                    values.append(buf.strip())
                    buf = ""
                    k += 1
                    while k < len(values_str) and values_str[k].isspace():
                        k += 1
                    continue
                else:
                    buf += ch
            else:
                buf += ch
            k += 1
        if buf.strip():
            values.append(buf.strip())
        inserts.setdefault(table_name, []).append(values)
        idx = statement_end + 1
    return inserts

def transform_to_cypher(sql_text):
    tables = parse_create_table_defs(sql_text)
    inserts = parse_inserts(sql_text)
    cypher_lines = []
    # Create nodes for each non-connector table
    for table, rows in inserts.items():
        if table.lower() == 't_connector':
            continue
        cols = tables.get(table, [])
        for row in rows:
            props = []
            for i, col in enumerate(cols):
                if i == 0 or col in ['Name', 'Notes', 'Note', 'Stereotype', 'Type', 'Object_Type', 'Connector_Type', 'Diagram_Type']:
                    if i >= len(row):
                        continue
                    val = row[i].strip()
                    if val.upper() == 'NULL' or val == "''":
                        continue  # skip null/empty
                    # Determine property key
                    if col in ['Note', 'Notes']:
                        key = 'Notes'
                    elif col in ['Object_Type', 'Connector_Type', 'Diagram_Type']:
                        key = 'Type'
                    else:
                        key = col[0].upper() + col[1:] if col else col
                    props.append(f"{key}: {val}")
            prop_map = "{ " + ", ".join(props) + " }" if props else "{}"
            cypher_lines.append(f"CREATE (:{table} {prop_map});")
    # Create relationships from t_connector
    if 't_connector' in inserts:
        cols = tables.get('t_connector', [])
        col_index = {col: idx for idx, col in enumerate(cols)}
        for row in inserts['t_connector']:
            start_id = row[col_index['Start_Object_ID']].strip() if 'Start_Object_ID' in col_index else None
            end_id = row[col_index['End_Object_ID']].strip() if 'End_Object_ID' in col_index else None
            conn_type = row[col_index['Connector_Type']].strip() if 'Connector_Type' in col_index else ''
            name = row[col_index['Name']].strip() if 'Name' in col_index else ''
            stereotype = row[col_index['Stereotype']].strip() if 'Stereotype' in col_index else ''
            notes = row[col_index['Notes']].strip() if 'Notes' in col_index else ''
            direction = row[col_index['Direction']].strip() if 'Direction' in col_index else ''
            # Relationship type label (uppercase, no spaces)
            if conn_type:
                base = conn_type[1:-1] if conn_type[0] == "'" and conn_type[-1] == "'" else conn_type
                base_clean = re.sub(r'\W+', '_', base)
                rel_type = base_clean.upper() if base_clean else 'RELATED_TO'
            else:
                rel_type = 'RELATED_TO'
            rel_props = []
            if name and name.upper() != 'NULL' and name != "''":
                rel_props.append(f"Name: {name}")
            if stereotype and stereotype.upper() != 'NULL' and stereotype != "''":
                rel_props.append(f"Stereotype: {stereotype}")
            if notes and notes.upper() != 'NULL' and notes != "''":
                rel_props.append(f"Notes: {notes}")
            if direction and direction.upper() != 'NULL' and direction != "''":
                rel_props.append(f"Direction: {direction}")
            if conn_type and conn_type.upper() != 'NULL' and conn_type != "''":
                rel_props.append(f"Type: {conn_type}")
            props_str = " { " + ", ".join(rel_props) + " }" if rel_props else ""
            if start_id and end_id:
                cypher_lines.append(
                    f"MATCH (src:t_object {{ Object_ID: {start_id} }}), (dst:t_object {{ Object_ID: {end_id} }}) "
                    f"CREATE (src)-[:{rel_type}{props_str}]->(dst);"
                )
    return "\n".join(cypher_lines)

if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python EA_to_Neo4j.py <input.sql> [output.cypher]")
        sys.exit(1)
    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) == 3 else None
    # Read the input SQL file
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            sql_text = f.read()
    except Exception as e:
        sys.stderr.write(f"Error reading input file: {e}\n")
        sys.exit(1)
    # Transform to Cypher
    cypher_output = transform_to_cypher(sql_text)
    # Write to output or stdout
    if output_path:
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(cypher_output)
        except Exception as e:
            sys.stderr.write(f"Error writing output file: {e}\n")
            sys.exit(1)
    else:
        print(cypher_output)
