from pathlib import Path
import os
import csv
import dbStructure
import geopandas as gpd

class ProcessingVoronoi:
    def __init__(self, in_dir, out_dir):
        self.in_dir = in_dir
        self.shapefile = in_dir / 'cities_Voronoi.shp'
        self.asof_date = ''
        #self.find_nearest_input_files(in_dir)
        self.out_dir =  out_dir

        self.polygons_list = []
        name, fields = self.read_fields(dbStructure.sql_create_city_polygons_table)
        self.polygons_header = fields
        self.polygons_table = name 

        self.points_list = []
        name, fields = self.read_fields(dbStructure.sql_create_city_points_table)
        self.points_header = fields
        self.points_table = name 

        if not os.path.isdir(self.out_dir / self.polygons_table):
            os.makedirs(self.out_dir / self.polygons_table)

        if not os.path.isdir(self.out_dir / self.points_table):
            os.makedirs(self.out_dir / self.points_table)

    def read_fields(self, sql_str):
        """Reads in the table name and field names from the dbStructure file.
        The dbStructure file should be the standard for the DB,
        and everything should reference it for the ground truth."""
        table_fields = []
        table_name = sql_str.split('\n')[0].rstrip().split(' ')[-1].replace('(', '')
        sql_list = sql_str.split('\n')[1:-1]
        for row in sql_list:
            field = row.lstrip().split(' ')[0].upper()
            if 'PRIMARY' in field or 'FOREIGN' in field:
                continue
            table_fields.append(field)
        return table_name, table_fields

    def run_steps(self):
        print("Processing local world city Voronoi data.")
        if not os.path.isdir(self.in_dir):
            print("\tThere is no data to process. Update the world city data before continuing.")
            return
        self.read_shapefile(self.shapefile)

        t_file = f"{self.polygons_table}.csv" 
        polygons_file = self.out_dir / self.polygons_table / t_file
        self.save_csv(self.polygons_list, self.polygons_header, polygons_file)

        t_file = f"{self.points_table}.csv" 
        points_file = self.out_dir / self.points_table / t_file
        self.save_csv(self.points_list, self.points_header, points_file)

    def read_shapefile(self, f_name):
        print(f"\tReading {f_name}")
        loc_df = gpd.read_file(f_name)
        for i,row in loc_df.iterrows():
            lat = row["LATITUDE"]
            lon = row["LONGITUDE"]
            city_name = row["NAME"].replace("'", "''")
            try:
                province_name = row["ADM1NAME"].replace("'", "''")
            except:
                province_name = None
            cc = row["ISO_A2"]
            poly_wkt = row["geometry"].wkt
            polygon_row = [city_name, province_name, cc, poly_wkt]
            self.polygons_list.append(polygon_row)

            points_row = [city_name, province_name, cc, lat, lon]
            self.points_list.append(points_row)

    def save_csv(self, data, header, f_name):
        print(f"\tSaving to {f_name}.")
        with open(f_name, 'w') as f:
            csv_writer = csv.writer(f, delimiter=',')
            csv_writer.writerow(header)
            for d in data:
                csv_writer.writerow(d)

if __name__ == "__main__":
    print("This script should not be run by itself. Run it through iGDB.py")
    input_dir = Path("../helper_data/cities_Voronoi")
    output_dir = Path("../processed")
    my_processor = ProcessingVoronoi(input_dir, output_dir)
    my_processor.run_steps()

