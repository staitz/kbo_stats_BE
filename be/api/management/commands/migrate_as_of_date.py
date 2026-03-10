"""
Django management command to migrate as_of_date from YYYYMMDD → YYYY-MM-DD.

Usage:
    python manage.py migrate_as_of_date
"""
from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Convert hitter_predictions.as_of_date from YYYYMMDD to YYYY-MM-DD format"

    def handle(self, *args, **options):
        with connection.cursor() as cursor:
            # Check current state
            cursor.execute(
                "SELECT as_of_date, COUNT(*) as c FROM hitter_predictions "
                "GROUP BY as_of_date ORDER BY as_of_date DESC LIMIT 10"
            )
            rows = cursor.fetchall()
            self.stdout.write("Current as_of_date values:")
            for r in rows:
                self.stdout.write(f"  '{r[0]}'  count={r[1]}")

            # Count rows that need migration
            cursor.execute(
                "SELECT COUNT(*) FROM hitter_predictions "
                "WHERE length(as_of_date) = 8 AND as_of_date NOT LIKE '%-%%'"
            )
            needs_migration = cursor.fetchone()[0]
            self.stdout.write(f"\nRows needing migration: {needs_migration}")

            if needs_migration > 0:
                cursor.execute(
                    """
                    UPDATE hitter_predictions
                    SET as_of_date =
                        substr(as_of_date, 1, 4) || '-' ||
                        substr(as_of_date, 5, 2) || '-' ||
                        substr(as_of_date, 7, 2)
                    WHERE length(as_of_date) = 8 AND as_of_date NOT LIKE '%-%%'
                    """
                )
                self.stdout.write(
                    self.style.SUCCESS(
                        f"\nMigration complete: {needs_migration} rows updated to YYYY-MM-DD format."
                    )
                )

                # Verify
                cursor.execute(
                    "SELECT as_of_date, COUNT(*) as c FROM hitter_predictions "
                    "GROUP BY as_of_date ORDER BY as_of_date DESC LIMIT 10"
                )
                after = cursor.fetchall()
                self.stdout.write("\nAfter migration:")
                for r in after:
                    self.stdout.write(f"  '{r[0]}'  count={r[1]}")
            else:
                self.stdout.write(self.style.WARNING("\nNo rows needed migration (already YYYY-MM-DD or table empty)."))
