import matplotlib

matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import time
import json
from pathlib import Path
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

from testfiles.mysql_test import mysql_tests
from testfiles.postgres_test import postgresql_tests
from testfiles.mongodb_test import mongo_tests
from testfiles.dynamodb_test import dynamo_tests


class DatabaseBenchmarkVisualizer:
    def __init__(self):
        self.results = {}
        self.colors = {
            'MySQL': '#1f77b4',
            'PostgreSQL': '#ff7f0e',
            'MongoDB': '#2ca02c',
            'DynamoDB': '#d62728'
        }
        self.crud_colors = {
            'CREATE': '#2E86AB',
            'READ': '#A23B72',
            'UPDATE': '#F18F01',
            'DELETE': '#C73E1D'
        }

    def load_results(self):
        results_files = {
            'MySQL': 'mysql_benchmark_results.json',
            'PostgreSQL': 'postgresql_benchmark_results.json',
            'MongoDB': 'mongodb_benchmark_results.json',
            'DynamoDB': 'dynamodb_benchmark_results.json'
        }

        self.results = {}

        for db_name, filename in results_files.items():
            try:
                if Path(filename).exists():
                    with open(filename, 'r') as f:
                        data = json.load(f)
                        self.results[db_name] = data
                        print(f"✅ Załadowano wyniki dla {db_name}: {len(data)} rozmiarów danych")
                else:
                    print(f"⚠️ Nie znaleziono pliku: {filename}")
                    self.results[db_name] = {}
            except Exception as e:
                print(f"❌ Błąd ładowania {filename}: {e}")
                self.results[db_name] = {}

    def run_all_tests(self):
        print("🚀 Rozpoczynanie testów wydajności baz danych...")

        databases = {
            'MySQL': mysql_tests,
            'PostgreSQL': postgresql_tests,
            'MongoDB': mongo_tests,
            'DynamoDB': dynamo_tests
        }

        for db_name, test_func in databases.items():
            print(f"\n📊 Testowanie {db_name}...")
            start_time = time.time()

            try:
                test_results = test_func()
                self.results[db_name] = test_results

                duration = time.time() - start_time
                print(f"✅ {db_name} ukończone w {duration:.2f}s")

            except Exception as e:
                print(f"❌ Błąd podczas testowania {db_name}: {e}")
                self.results[db_name] = {}

        print("\n📈 Generowanie wykresów porównawczych...")
        self.generate_all_charts()

    def create_performance_overview_by_dataset(self):
        data_sizes = list(next(iter(self.results.values())).keys()) if self.results else []

        for data_size in data_sizes:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle(f'Wydajność baz danych - Rozmiar danych: {data_size:,}',
                         fontsize=16, fontweight='bold')

            operations = ['CREATE', 'READ', 'UPDATE', 'DELETE']

            for idx, operation in enumerate(operations):
                ax = [ax1, ax2, ax3, ax4][idx]
                db_names = []
                times = []

                for db_name, db_data in self.results.items():
                    if db_data and data_size in db_data and operation in db_data[data_size]:
                        db_names.append(db_name)
                        times.append(db_data[data_size][operation])

                if db_names and times:
                    bars = ax.bar(db_names, times, color=self.crud_colors[operation], alpha=0.8)

                    for bar, time_val in zip(bars, times):
                        height = bar.get_height()
                        ax.text(bar.get_x() + bar.get_width() / 2., height + height * 0.01,
                                f'{time_val:.3f}s', ha='center', va='bottom', fontweight='bold')

                    ax.set_ylabel('Czas wykonania (s)', fontweight='bold')
                    ax.set_title(f'Operacja {operation}', fontweight='bold')
                    ax.set_yscale('log')
                    plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
                    ax.grid(True, alpha=0.3, axis='y')

            plt.tight_layout()
            plt.savefig(f'performance_overview_{data_size}.png', dpi=300, bbox_inches='tight')
            plt.show()

    def create_scalability_analysis(self):
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Analiza skalowalności baz danych', fontsize=16, fontweight='bold')

        operations = ['CREATE', 'READ', 'UPDATE', 'DELETE']

        for idx, operation in enumerate(operations):
            ax = axes[idx // 2, idx % 2]

            for db_name, db_data in self.results.items():
                if not db_data:
                    continue

                sizes = list(db_data.keys())
                times = [db_data[size].get(operation, 0) for size in sizes]

                if len(sizes) > 1 and any(times):
                    ax.loglog(sizes, times, marker='o', linewidth=2, markersize=8,
                              label=db_name, color=self.colors[db_name], alpha=0.8)
                elif sizes and times[0] > 0:
                    ax.scatter(sizes, times, s=100, label=db_name,
                               color=self.colors[db_name], alpha=0.8)

            ax.set_xlabel('Rozmiar danych', fontweight='bold')
            ax.set_ylabel('Czas wykonania (s)', fontweight='bold')
            ax.set_title(f'Skalowalność - {operation}', fontweight='bold')
            ax.legend()
            ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig('scalability_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

    def create_performance_heatmap(self):
        data_for_heatmap = []

        for db_name, db_data in self.results.items():
            for size, ops in db_data.items():
                for operation, time_val in ops.items():
                    if operation in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
                        data_for_heatmap.append({
                            'Database': db_name,
                            'Operation': operation,
                            'Time': time_val
                        })

        if not data_for_heatmap:
            print("Brak danych do utworzenia mapy cieplnej")
            return

        df = pd.DataFrame(data_for_heatmap)

        aggregated_df = df.groupby(['Database', 'Operation'])['Time'].mean().reset_index()

        pivot_table = aggregated_df.pivot_table(
            values='Time',
            index='Database',
            columns='Operation',
            fill_value=0
        )

        fig, ax = plt.subplots(figsize=(10, 6))
        sns.heatmap(pivot_table, annot=True, fmt='.4f', cmap='YlOrRd',
                    cbar_kws={'label': 'Średni czas wykonania (s)'}, ax=ax)

        ax.set_title('Mapa cieplna wydajności - średnie czasy operacji CRUD',
                     fontsize=16, fontweight='bold')
        ax.set_xlabel('Typ operacji', fontweight='bold')
        ax.set_ylabel('Baza danych', fontweight='bold')

        plt.xticks(rotation=0)
        plt.yticks(rotation=0)
        plt.tight_layout()
        plt.savefig('performance_heatmap_by_operation.png', dpi=300, bbox_inches='tight')
        plt.show()

    def create_comparative_radar_chart(self):
        operations = ['CREATE', 'READ', 'UPDATE', 'DELETE']
        angles = np.linspace(0, 2 * np.pi, len(operations), endpoint=False).tolist()
        angles += angles[:1]

        fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))

        for db_name, db_data in self.results.items():
            if not db_data:
                continue

            avg_times = []
            for operation in operations:
                times = []
                for size_data in db_data.values():
                    if operation in size_data:
                        times.append(size_data[operation])

                if times:
                    avg_times.append(1 / np.mean(times))
                else:
                    avg_times.append(0)

            if any(avg_times):
                avg_times += avg_times[:1]

                ax.plot(angles, avg_times, 'o-', linewidth=2,
                        label=db_name, color=self.colors[db_name])
                ax.fill(angles, avg_times, alpha=0.25, color=self.colors[db_name])

        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(operations)
        ax.set_title('Porównanie wydajności baz danych\n(wyższa wartość = lepsza wydajność)',
                     fontweight='bold', pad=20)
        ax.legend(loc='upper right', bbox_to_anchor=(1.2, 1.0))
        ax.grid(True)

        plt.tight_layout()
        plt.savefig('comparative_radar_chart.png', dpi=300, bbox_inches='tight')
        plt.show()

    def create_performance_summary_table(self):
        fig, ax = plt.subplots(figsize=(14, 8))
        ax.axis('tight')
        ax.axis('off')

        summary_data = []
        headers = ['Baza danych', 'Rozmiar danych', 'CREATE (s)', 'READ (s)', 'UPDATE (s)', 'DELETE (s)', 'Średnia (s)']

        for db_name, db_data in self.results.items():
            for size, ops in db_data.items():
                row = [
                    db_name,
                    f"{size:,}",
                    f"{ops.get('CREATE', 0):.4f}",
                    f"{ops.get('READ', 0):.4f}",
                    f"{ops.get('UPDATE', 0):.4f}",
                    f"{ops.get('DELETE', 0):.4f}",
                    f"{np.mean(list(ops.values())):.4f}"
                ]
                summary_data.append(row)

        if summary_data:
            table = ax.table(cellText=summary_data, colLabels=headers,
                             cellLoc='center', loc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(9)
            table.scale(1.2, 1.5)

            for i in range(len(headers)):
                table[(0, i)].set_facecolor('#4CAF50')
                table[(0, i)].set_text_props(weight='bold', color='white')

        ax.set_title('Podsumowanie wyników testów wydajności',
                     fontsize=16, fontweight='bold', pad=20)

        plt.tight_layout()
        plt.savefig('performance_summary_table.png', dpi=300, bbox_inches='tight')
        plt.show()

    def create_test_comparison_charts(self):
        if not self.results:
            print("Brak danych do analizy")
            return

        test_names = [
            'test_insert_book_genre',
            'test_insert_user',
            'test_insert_publisher_and_author',
            'test_insert_order_and_return',
            'test_insert_book_rating_group_by',
            'test_insert_book_rating_join',
            'test_get_order_with_book_and_author',
            'test_get_average_book_rating_above',
            'test_get_genre_book_counts_group_by',
            'test_get_users_and_orders_join',
            'test_update_genre_popularity',
            'test_update_user_location',
            'test_update_genre_popularity_group_by',
            'test_update_user_with_order_join',
            'test_delete_genre_by_id',
            'test_delete_user_by_id',
            'test_delete_books_with_few_ratings_group_by',
            'test_delete_orders_with_user_join'
        ]

        test_mapping = {
            'test_insert_book_rating_group_by': {
                'MongoDB': 'test_insert_book_rating_aggregation'
            },
            'test_insert_book_rating_join': {
                'MongoDB': 'test_insert_book_rating_with_lookup'
            },
            'test_get_order_with_book_and_author': {
                'MongoDB': 'test_get_order_with_book_and_author_lookup'
            },
            'test_get_average_book_rating_above': {
                'MongoDB': 'test_get_average_book_rating_above_aggregation'
            },
            'test_get_genre_book_counts_group_by': {
                'MongoDB': 'test_get_genre_book_counts_aggregation'
            },
            'test_get_users_and_orders_join': {
                'MongoDB': 'test_get_users_and_orders_lookup'
            },
            'test_update_genre_popularity_group_by': {
                'MongoDB': 'test_update_genre_popularity_aggregation'
            },
            'test_update_user_with_order_join': {
                'MongoDB': 'test_update_user_with_order_lookup'
            },
            'test_delete_books_with_few_ratings_group_by': {
                'MongoDB': 'test_delete_books_with_few_ratings_aggregation'
            },
            'test_delete_orders_with_user_join': {
                'MongoDB': 'test_delete_orders_with_user_lookup'
            }
        }

        print(f"Tworzenie wykresów dla {len(test_names)} testów...")

        for test_name in test_names:
            print(f"Tworzenie wykresu dla: {test_name}")
            self._create_single_test_average_chart(test_name, test_mapping)

    def _create_single_test_average_chart(self, test_name, test_mapping):
        fig, ax = plt.subplots(figsize=(12, 8))

        db_averages = {}

        for db_name, db_data in self.results.items():
            if not db_data:
                continue

            actual_test_name = test_name
            if test_name in test_mapping and db_name in test_mapping[test_name]:
                actual_test_name = test_mapping[test_name][db_name]

            test_times = []

            for size, size_data in db_data.items():
                if actual_test_name in size_data:
                    if actual_test_name not in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
                        test_times.append(size_data[actual_test_name])

            if test_times:
                db_averages[db_name] = {
                    'mean': np.mean(test_times),
                    'std': np.std(test_times) if len(test_times) > 1 else 0,
                    'count': len(test_times)
                }

        if not db_averages:
            print(f"⚠️ Brak danych dla testu: {test_name}")
            plt.close(fig)
            return

        db_names = list(db_averages.keys())
        means = [db_averages[db]['mean'] for db in db_names]
        stds = [db_averages[db]['std'] for db in db_names]

        bars = ax.bar(db_names, means,
                      yerr=stds,
                      capsize=5,
                      color=[self.colors.get(db, f'C{i}') for i, db in enumerate(db_names)],
                      alpha=0.8,
                      error_kw={'linewidth': 2, 'capthick': 2})

        for bar, mean_val, std_val, db_name in zip(bars, means, stds, db_names):
            height = bar.get_height()

            if std_val > 0:
                text = f'{mean_val:.3f}s\n±{std_val:.3f}'
            else:
                text = f'{mean_val:.3f}s'

            ax.text(bar.get_x() + bar.get_width() / 2.,
                    height + std_val + height * 0.02,
                    text,
                    ha='center', va='bottom',
                    fontweight='bold', fontsize=10)

            ax.text(bar.get_x() + bar.get_width() / 2.,
                    height * 0.05,
                    f'n={db_averages[db_name]["count"]}',
                    ha='center', va='bottom',
                    fontsize=8, color='white', fontweight='bold')

        ax.set_ylabel('Średni czas wykonania (s)', fontweight='bold', fontsize=12)
        ax.set_title(f'Średnie czasy wykonania - Test: {test_name}',
                     fontweight='bold', fontsize=14)
        ax.set_yscale('log')
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
        ax.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()

        safe_filename = "".join(c for c in test_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_filename = safe_filename.replace(' ', '_').lower()

        plt.savefig(f'test_average_{safe_filename}.png', dpi=300, bbox_inches='tight')
        plt.show()

    def debug_available_tests(self):
        print("\n=== DEBUGOWANIE DOSTĘPNYCH TESTÓW ===")
        for db_name, db_data in self.results.items():
            print(f"\n📊 {db_name}:")
            if db_data:
                for size, size_data in db_data.items():
                    print(f"  📈 Dataset {size}:")
                    for test_name in sorted(size_data.keys()):
                        print(f"    ✅ {test_name}")
            else:
                print("  ❌ Brak danych")

    def debug_individual_tests(self):
        print("\n=== DEBUGOWANIE POSZCZEGÓLNYCH TESTÓW ===")
        for db_name, db_data in self.results.items():
            print(f"\n📊 {db_name}:")
            if db_data:
                for size, size_data in db_data.items():
                    print(f"  📈 Dataset {size}:")
                    individual_tests = [key for key in size_data.keys()
                                        if key not in ['CREATE', 'READ', 'UPDATE', 'DELETE']]
                    if individual_tests:
                        for test_name in sorted(individual_tests):
                            print(f"    ✅ {test_name}: {size_data[test_name]:.4f}s")
                    else:
                        print("    ❌ Brak danych poszczególnych testów")
            else:
                print("  ❌ Brak danych")

    def generate_all_charts(self):
        self.debug_available_tests()
        self.debug_individual_tests()

        print("Tworzenie wykresów wydajności dla każdego rozmiaru danych...")
        self.create_performance_overview_by_dataset()

        print("Tworzenie wykresów średnich czasów dla każdego z 18 testów...")
        self.create_test_comparison_charts()

        print("Analiza skalowalności...")
        self.create_scalability_analysis()

        print("Mapa cieplna wydajności...")
        self.create_performance_heatmap()

        print("Wykres radarowy...")
        self.create_comparative_radar_chart()

        print("Tabela podsumowująca...")
        self.create_performance_summary_table()

        print("✅ Wszystkie wykresy zostały wygenerowane!")
        self.print_summary()

    def print_summary(self):
        print("\n" + "=" * 60)
        print("📊 PODSUMOWANIE TESTÓW WYDAJNOŚCI BAZ DANYCH")
        print("=" * 60)

        for db_name, db_data in self.results.items():
            if db_data:
                print(f"\n🔹 {db_name}:")
                for size, ops in db_data.items():
                    if isinstance(ops, dict) and ops:
                        avg_time = np.mean(list(ops.values()))
                        print(f"  Rozmiar {size:,}: średni czas {avg_time:.4f}s")
                        for op, time_val in ops.items():
                            print(f"    {op}: {time_val:.4f}s")

        test_names = [
            'test_insert_book_genre', 'test_insert_user', 'test_insert_publisher_and_author',
            'test_insert_order_and_return', 'test_insert_book_rating_group_by', 'test_insert_book_rating_join',
            'test_get_order_with_book_and_author', 'test_get_average_book_rating_above',
            'test_get_genre_book_counts_group_by',
            'test_get_users_and_orders_join', 'test_update_genre_popularity', 'test_update_user_location',
            'test_update_genre_popularity_group_by', 'test_update_user_with_order_join', 'test_delete_genre_by_id',
            'test_delete_user_by_id', 'test_delete_books_with_few_ratings_group_by', 'test_delete_orders_with_user_join'
        ]

        charts = []
        data_sizes = list(next(iter(self.results.values())).keys()) if self.results else []

        for size in data_sizes:
            charts.append(f"performance_overview_{size}.png")

        for test_name in test_names:
            safe_filename = "".join(c for c in test_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            safe_filename = safe_filename.replace(' ', '_').lower()
            charts.append(f"test_average_{safe_filename}.png")

        charts.extend([
            "scalability_analysis.png",
            "performance_heatmap.png",
            "comparative_radar_chart.png",
            "performance_summary_table.png"
        ])

        print(f"\n📁 Wykresy zapisane w bieżącym katalogu:")
        for chart in charts:
            print(f"  ✓ {chart}")


def main():
    print("🚀 System analizy wydajności baz danych")
    print("=" * 50)

    visualizer = DatabaseBenchmarkVisualizer()

    try:
        print("📂 Ładowanie wyników z plików JSON...")
        visualizer.load_results()

        if not any(visualizer.results.values()):
            print("⚠️ Brak danych - uruchamianie testów...")
            visualizer.run_all_tests()
        else:
            print("📈 Generowanie wykresów z załadowanych danych...")
            visualizer.generate_all_charts()

        print(f"\n✅ Analiza zakończona pomyślnie!")
        print(f"🕒 Czas: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except KeyboardInterrupt:
        print("\n⚠️ Analiza przerwana przez użytkownika")
    except Exception as e:
        print(f"\n❌ Błąd podczas analizy: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()