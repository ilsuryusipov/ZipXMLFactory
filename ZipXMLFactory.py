import os
import csv
import uuid
import time
import random
import string
import shutil
import zipfile
import logging
import multiprocessing
from xml.etree.ElementTree import ElementTree, Element, SubElement


sh = logging.StreamHandler()
sh.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# set INFO level to minimize output info
# logger.setLevel(logging.INFO)
logger.addHandler(sh)


def random_string():
    return "".join(random.choices(string.ascii_lowercase,
        k = random.randrange(10, 20)))


class ZipPackProducer:
    def __init__(self):
        logger.info("="*10 + " ZipPackProducer contructing " + "="*10)

    def _generate_xml_tree(self):
        try:
            root = Element("root")
            root.append(Element("var", name="id", value=str(uuid.uuid4())))
            root.append(Element("var", name="level", value=str(random.randrange(1, 101))))
            _objects = Element("objects")
            root.append(_objects)

            for i in range(random.randrange(1, 11)):
                SubElement(_objects, "object", name=random_string())

            return ElementTree(root)
        except Exception as e:
            logger.error(str(e))
            raise e

    def _compress_to_zip(self, input_folder_path, output_folder_path):
        try:
            archive_basename = os.path.basename(input_folder_path)
            output_archive = os.path.join(output_folder_path, f"{archive_basename}.zip")
            with zipfile.ZipFile(output_archive, mode="w") as _zip:
                for _file in os.listdir(input_folder_path):
                    _zip.write(os.path.join(input_folder_path, _file), _file)
        except Exception as e:
            logger.error(str(e))
            raise e

    def run(self, output_zip_pack_dir, dirs_in_pack_count=1, files_in_dir_count=1):
        start_time = time.time()

        try:
            tmp_dir = os.path.join("/tmp", f"zippackproducer_{random_string()}")
            if not os.path.exists(tmp_dir):
                os.makedirs(tmp_dir)
                logger.info(f"{tmp_dir} temp dir created")
            else:
                logger.warning(f"{tmp_dir} dir already exists")
        except Exception as e:
            logger.error(str(e))
            raise e
        
        try:
            if os.path.exists(output_zip_pack_dir):
                shutil.rmtree(output_zip_pack_dir)
                logger.warning(f"existing {output_zip_pack_dir} dir removed")
            os.makedirs(output_zip_pack_dir)
            logger.info(f"{output_zip_pack_dir} dir created")
        except Exception as e:
            logger.error(str(e))
            shutil.rmtree(tmp_dir)
            logger.warning(f"{tmp_dir} temp dir removed")
            raise e

        try:
            for i in range(dirs_in_pack_count):
                current_xml_storage_dir = os.path.join(tmp_dir, f"dir_{i}")
                os.makedirs(current_xml_storage_dir)
                logger.info(f"{current_xml_storage_dir} dir created")
                for j in range(files_in_dir_count):
                    current_xml_file = os.path.join(current_xml_storage_dir, f"file_{i}_{j}.xml")
                    xml_tree = self._generate_xml_tree()
                    xml_tree.write(current_xml_file, "utf-8")
                    logger.info(f"{current_xml_file} created")
                self._compress_to_zip(current_xml_storage_dir, output_zip_pack_dir)
                logger.info(f"{current_xml_storage_dir} zipped to {output_zip_pack_dir}")
        except Exception as e:
            logger.error(str(e))
            shutil.rmtree(tmp_dir)
            logger.warning(f"{tmp_dir} temp dir removed")
            shutil.rmtree(output_zip_pack_dir)
            logger.warning(f"{output_zip_pack_dir} dir removed")
            raise e
        
        try:
            shutil.rmtree(tmp_dir)
            logger.info(f"{tmp_dir} temp dir removed")
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.error(str(e))
            raise e

        end_time = time.time()
        logger.info(f"Total producing time: {round(end_time - start_time, 3)} seconds")


class ZipPackHandler:
    def __init__(self, workers_count = None):
        logger.info("="*10 + " ZipPackHandler contructing " + "="*10)
        if workers_count:
            self.workers_count = workers_count
        else:
            self.workers_count = multiprocessing.cpu_count()

    def _unzip(self, zip_file, output_dir):
        with zipfile.ZipFile(zip_file) as _zip:
            _zip.extractall(output_dir)

    def _start_unzip_pool(self, zip_pack_dir, output_dir):
        workers_input = [(os.path.join(zip_pack_dir, i), output_dir,) for i in os.listdir(zip_pack_dir)]
        pool = multiprocessing.Pool(processes=self.workers_count)
        logger.info("parallel unzip procedure starting")
        pool.starmap(self._unzip, workers_input)
        pool.close()
        pool.join()
        logger.info("parallel unzip procedure finished")

    def _aggregate_levels_csv(self, csv_file, queue, stop_event):
        with open(csv_file, "w") as f:
            writer = csv.writer(f, delimiter = ",")
            writer.writerow(["id", "level"])
            while not stop_event.is_set():
                if not queue.empty():
                    record = queue.get()
                    logger.debug(record)
                    writer.writerow([record["id"], record["level"]])

    def _aggregate_objects_csv(self, csv_file, queue, stop_event):
        with open(csv_file, "w") as f:
            writer = csv.writer(f, delimiter = ",")
            writer.writerow(["id", "object_name"])
            while not stop_event.is_set():
                if not queue.empty():
                    record = queue.get()
                    logger.debug(record)
                    for object_name in record["object_names"]:
                        writer.writerow([record["id"], object_name])

    def _parse_xml(self, xml_file, levels_queue, objects_queue):
        logger.info(f"{multiprocessing.current_process().name} : {xml_file} parsing")
        tree = ElementTree(file=xml_file)
        tree_iterator = tree.iter()

        levels_dict = {}
        objects_dict = {"object_names": []}

        for element in tree_iterator:
            if element.tag == "var" and element.attrib["name"] == "id":
                levels_dict["id"] = element.attrib["value"]
                objects_dict["id"] = element.attrib["value"]
            if element.tag == "var" and element.attrib["name"] == "level":
                levels_dict["level"] = element.attrib["value"]
            if element.tag == "object":
                objects_dict["object_names"].append(element.attrib["name"])

        levels_queue.put(levels_dict)
        objects_queue.put(objects_dict)
    
    def _start_parsing_xml_pool(self, xml_files_storage, levels_csv_aggregator_queue, objects_csv_aggregator_queue):
        workers_input = [(os.path.join(xml_files_storage, i), levels_csv_aggregator_queue,
            objects_csv_aggregator_queue) for i in os.listdir(xml_files_storage)]
        pool = multiprocessing.Pool(processes=self.workers_count)
        logger.info("parallel xml parsing procedure starting")
        pool.starmap(self._parse_xml, workers_input)

        pool.close()
        pool.join()
        logger.info("parallel xml parsing procedure finished")

    def run(self, input_zip_pack_dir, output_levels_csv_file, output_objects_csv_file):
        start_time = time.time()

        try:
            tmp_dir = os.path.join("/tmp", f"zippackhandler_{random_string()}")
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)
                logger.warning(f"existing {tmp_dir} dir removed")
            os.makedirs(tmp_dir)
            logger.info(f"{tmp_dir} temp dir created")
        except Exception as e:
            logger.error(str(e))
            raise e

        self._start_unzip_pool(input_zip_pack_dir, tmp_dir)

        manager = multiprocessing.Manager()

        levels_csv_aggregator_queue = manager.Queue()
        levels_csv_aggregator_stop = manager.Event()
        objects_csv_aggregator_queue = manager.Queue()
        objects_csv_aggregator_stop = manager.Event()

        levels_csv_aggregator_process = multiprocessing.Process(target=self._aggregate_levels_csv,
            args=(output_levels_csv_file, levels_csv_aggregator_queue, levels_csv_aggregator_stop,))
        levels_csv_aggregator_process.daemon = True
        levels_csv_aggregator_process.start()
        logger.info("levels_csv_aggregator started")

        objects_csv_aggregator_process = multiprocessing.Process(target=self._aggregate_objects_csv,
            args=(output_objects_csv_file, objects_csv_aggregator_queue, objects_csv_aggregator_stop))
        objects_csv_aggregator_process.daemon = True
        objects_csv_aggregator_process.start()
        logger.info("objects_csv_aggregator started")

        self._start_parsing_xml_pool(tmp_dir, levels_csv_aggregator_queue, objects_csv_aggregator_queue)

        levels_csv_aggregator_stop.set()
        levels_csv_aggregator_process.join()
        levels_csv_aggregator_process.close()
        logger.info("levels_csv_aggregator finished")

        objects_csv_aggregator_stop.set()
        objects_csv_aggregator_process.join()
        objects_csv_aggregator_process.close()
        logger.info("objects_csv_aggregator finished")

        manager.shutdown()
        logger.info("multiprocessing manager closed")

        try:
            shutil.rmtree(tmp_dir)
            logger.info(f"{tmp_dir} temp dir removed")
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.error(str(e))
            raise e

        end_time = time.time()
        logger.info(f"Total handling time: {round(end_time - start_time, 3)} seconds")


def main():
    zpp = ZipPackProducer()
    zip_pack_dir = os.path.join(os.path.abspath(os.path.curdir), "zip_pack")
    zpp.run(
        output_zip_pack_dir=zip_pack_dir,
        dirs_in_pack_count=50,
        files_in_dir_count=100
    )

    zph = ZipPackHandler()
    # or set certain workers count
    # zph = ZipPackHandler(1)
    zph.run(
        input_zip_pack_dir=zip_pack_dir,
        output_levels_csv_file= os.path.join(os.path.abspath(os.path.curdir), "id+level.csv"),
        output_objects_csv_file=os.path.join(os.path.abspath(os.path.curdir), "id+object_name.csv")
    )


if __name__ == '__main__':
    main()
