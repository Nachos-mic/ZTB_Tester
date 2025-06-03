import matplotlib
matplotlib.use('TkAgg')  # lub 'Qt5Agg'
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import time
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

    def run_all_tests(self):

        print(" Rozpoczynanie testów wydajności baz danych...")

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
                # Uruchomienie testów i pobranie wyników
                test_results = test_func()
                self.results[db_name] = test_results

                duration = time.time() - start_time
                print(f"✅ {db_name} ukończone w {duration:.2f}s")

            except Exception as e:
                print(f"❌ Błąd podczas testowania {db_name}: {e}")
                self.results[db_name] = {}

        print("\n Generowanie wykresów porównawczych...")
        self.generate_all_charts()

    def create_performance_overview(self):

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Przegląd wydajności baz danych - Analiza CRUD', fontsize=16, fontweight='bold')

        operations = ['CREATE', 'READ', 'UPDATE', 'DELETE']

        for idx, operation in enumerate(operations):
            ax = [ax1, ax2, ax3, ax4][idx]

            db_names = []
            avg_times = []

            for db_name, db_data in self.results.items():
                if db_data:
                    times = []
                    for size_data in db_data.values():
                        if operation in size_data:
                            times.append(size_data[operation])

                    if times:
                        avg_time = np.mean(times)
                        db_names.append(db_name)
                        avg_times.append(avg_time)

            if db_names and avg_times:
                bars = ax.bar(db_names, avg_times, color=self.crud_colors[operation], alpha=0.8)

                for bar, time_val in zip(bars, avg_times):
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width() / 2., height + height * 0.01,
                            f'{time_val:.3f}s', ha='center', va='bottom', fontweight='bold')

                ax.set_ylabel('Średni czas (s)', fontweight='bold')
                ax.set_title(f'Operacja {operation}', fontweight='bold')
                ax.set_yscale('log')
                plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
                ax.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()
        plt.savefig('performance_overview.png', dpi=300, bbox_inches='tight')
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
                    data_for_heatmap.append({
                        'Database': db_name,
                        'Data_Size': size,
                        'Operation': operation,
                        'Time': time_val
                    })

        if not data_for_heatmap:
            print("Brak danych do utworzenia mapy cieplnej")
            return

        df = pd.DataFrame(data_for_heatmap)
        pivot_table = df.pivot_table(values='Time', index=['Database', 'Data_Size'],
                                     columns='Operation', fill_value=0)

        fig, ax = plt.subplots(figsize=(12, 8))
        sns.heatmap(pivot_table, annot=True, fmt='.4f', cmap='YlOrRd',
                    cbar_kws={'label': 'Czas wykonania (s)'}, ax=ax)

        ax.set_title('Mapa cieplna wydajności baz danych', fontsize=16, fontweight='bold')
        ax.set_xlabel('Operacje CRUD', fontweight='bold')
        ax.set_ylabel('Baza danych i rozmiar danych', fontweight='bold')

        plt.xticks(rotation=0)
        plt.yticks(rotation=0)
        plt.tight_layout()
        plt.savefig('performance_heatmap.png', dpi=300, bbox_inches='tight')
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

    def generate_all_charts(self):

        print("Tworzenie przeglądu wydajności...")
        self.create_performance_overview()

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
                    avg_time = np.mean(list(ops.values()))
                    print(f"   Rozmiar {size:,}: średni czas {avg_time:.4f}s")
                    for op, time_val in ops.items():
                        print(f"     {op}: {time_val:.4f}s")

        charts = [
            "performance_overview.png",
            "scalability_analysis.png",
            "performance_heatmap.png",
            "comparative_radar_chart.png",
            "performance_summary_table.png"
        ]

        print(f"\n📁 Wykresy zapisane w bieżącym katalogu:")
        for chart in charts:
            print(f"   ✓ {chart}")


def main():

    print("System analizy wydajności baz danych")
    print("=" * 50)

    visualizer = DatabaseBenchmarkVisualizer()

    try:
        visualizer.run_all_tests()

        print(f"\nAnaliza zakończona pomyślnie!")
        print(f"Czas: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    except KeyboardInterrupt:
        print("\n⚠️ Analiza przerwana przez użytkownika")
    except Exception as e:
        print(f"\n❌ Błąd podczas analizy: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
