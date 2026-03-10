import os

script_dir = os.path.dirname(os.path.abspath(__file__))
sql_dir_ = os.path.abspath("sql/candlesticks.sql")
sql_dir = os.path.abspath(os.path.join(script_dir, "..", ".."))

print(script_dir)
print(sql_dir_)
