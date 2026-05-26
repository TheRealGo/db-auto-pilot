from __future__ import annotations

import sqlite3
import unittest

from app.services.querying import QueryGenerationError, fallback_query, run_query, run_query_with_columns, schema_prompt, validate_select_sql, validate_table_scope


class QueryingUnittest(unittest.TestCase):
    def test_grouped_sum_fallback(self) -> None:
        with sqlite3.connect(":memory:") as conn:
            conn.execute("create table sales(department text, sales real)")
            conn.executemany("insert into sales values (?, ?)", [("営業", 100), ("営業", 50), ("開発", 25)])
            sql, explanation = fallback_query("部署別の売上合計を見たい", conn, "sales", limit=20)
            self.assertIn("sum", sql.lower())
            self.assertIn("limit 20", sql.lower())
            self.assertIn("合計", explanation)
            self.assertEqual(run_query(conn, sql)[0]["total"], 150)

    def test_count_and_average_fallbacks(self) -> None:
        with sqlite3.connect(":memory:") as conn:
            conn.execute("create table sales(department text, sales real)")
            conn.executemany("insert into sales values (?, ?)", [("営業", 100), ("営業", 50), ("開発", 25)])

            count_sql, count_explanation = fallback_query("行数を知りたい", conn, "sales", limit=20)
            self.assertIn("count(*)", count_sql.lower())
            self.assertIn("行数", count_explanation)
            self.assertEqual(run_query(conn, count_sql)[0]["row_count"], 3)

            average_sql, average_explanation = fallback_query("部署別の売上平均", conn, "sales", limit=20)
            self.assertIn("avg", average_sql.lower())
            self.assertIn("平均", average_explanation)
            self.assertEqual(run_query(conn, average_sql)[0]["average"], 75)

    def test_select_guard(self) -> None:
        self.assertEqual(validate_select_sql("select * from sales", default_limit=7), "select * from sales limit 7")
        self.assertEqual(validate_select_sql("with x as (select 1) select * from x", default_limit=7), "with x as (select 1) select * from x limit 7")
        self.assertEqual(validate_select_sql("select * from sales limit 3", default_limit=7), "select * from sales limit 3")

    def test_rejects_unsafe_sql(self) -> None:
        for sql in [
            "select * from sales;",
            "select * from sales -- comment",
            "delete from sales",
            "pragma table_info(sales)",
        ]:
            with self.subTest(sql=sql):
                with self.assertRaises(QueryGenerationError):
                    validate_select_sql(sql)

    def test_empty_results_preserve_columns(self) -> None:
        with sqlite3.connect(":memory:") as conn:
            conn.execute("create table sales(department text, sales real)")
            rows, columns = run_query_with_columns(conn, "select department, sales from sales where sales > 10")
            self.assertEqual(rows, [])
            self.assertEqual(columns, ["department", "sales"])

    def test_run_query_enforces_max_rows_against_existing_limit(self) -> None:
        with sqlite3.connect(":memory:") as conn:
            conn.execute("create table sales(id integer)")
            conn.executemany("insert into sales values (?)", [(1,), (2,), (3,)])
            with self.assertRaises(QueryGenerationError):
                run_query_with_columns(conn, "select * from sales limit 3", max_rows=2)
            rows, columns = run_query_with_columns(conn, "select * from sales limit 3", max_rows=3)
            self.assertEqual(columns, ["id"])
            self.assertEqual([row["id"] for row in rows], [1, 2, 3])

    def test_sqlite_authorizer_blocks_disallowed_table_reads(self) -> None:
        with sqlite3.connect(":memory:") as conn:
            conn.execute("create table allowed(id integer)")
            conn.execute("create table metadata(secret text)")
            self.assertEqual(run_query_with_columns(conn, "select * from allowed", {"allowed"})[1], ["id"])
            with self.assertRaises(QueryGenerationError):
                run_query_with_columns(conn, "select * from metadata", {"allowed"})
            with self.assertRaises(QueryGenerationError):
                run_query_with_columns(conn, "select 1", {"allowed"})

    def test_validate_table_scope_uses_sqlite_authorizer_for_cte_and_joins(self) -> None:
        with sqlite3.connect(":memory:") as conn:
            conn.execute("create table allowed(id integer)")
            conn.execute("create table other(id integer)")
            validate_table_scope(conn, "with src as (select * from allowed) select * from src", {"allowed"})
            with self.assertRaises(QueryGenerationError):
                validate_table_scope(conn, "select a.id from allowed a join other o on a.id = o.id", {"allowed"})
            with self.assertRaises(QueryGenerationError):
                validate_table_scope(conn, "select 1", {"allowed"})

    def test_schema_prompt_scopes_to_selected_dataset_table_and_excludes_metadata(self) -> None:
        with sqlite3.connect(":memory:") as conn:
            conn.execute("create table dataset_a(department text, sales real)")
            conn.execute("create table dataset_b(secret_column text)")
            conn.execute("create table materialization_runs(error text)")
            prompt = schema_prompt(conn, {"dataset_a"})
            self.assertIn("dataset_a(department TEXT, sales REAL)", prompt)
            self.assertNotIn("dataset_b", prompt)
            self.assertNotIn("materialization_runs", prompt)


if __name__ == "__main__":
    unittest.main()
