#! python3
# -*- coding: utf-8 -*-

import re
from pathlib import Path


class LogParser:
    def __init__(self, root_log_dir, logger):
        self.root_log_dir = root_log_dir
        self.logger = logger


    def get_dictionary(self, project_name):
        self.project_name = project_name
        self.latest_log = self.find_latest_log_file()
        if not self.latest_log:
            self.logger.critical(f"360sync LOG parser - {self.project_name} - Aucun fichier correspondant trouvé.")
            return
        self.logger.debug(f"360sync LOG parser - {self.project_name} - Fichier sélectionné : {self.latest_log.name}")
        self.logger.debug(f"360sync LOG parser - {self.project_name} - Parsing des blocs…")
        self.blocks = self.parse_log_blocks()
        self.nbBlocs = len(self.blocks)
        self.logger.debug(f"-> {self.nbBlocs} blocs found")
        return self.nbBlocs


    def display(self):
        self.logger.info(f"360sync LOG parser - {self.project_name} - Nombre de blocs trouvés : {self.nbBlocs}")
        if self.blocks:
            self.logger.info("360sync LOG parser - Résultat :")
            for entry in self.blocks:
                if not entry["mapping_name"]:
                    continue
                self.logger.info(f'360sync LOG parser - {entry["date"]} - {entry["mapping_name"]} : {entry["source"]} > {entry["destination"]} = {entry["final_status"]}')


    def build_filename_regex(self):
        escaped_project = re.escape(self.project_name)
        return re.compile(
            rf"^{escaped_project}_[^_]+_[^_]+_\d{{4}}-\d{{2}}-\d{{2}}\.txt$"
        )


    def parse_log_line(self, line: str):
        log_pattern = re.compile(
            r"(?P<date>\d{4}-\d{2}-\d{2})\s+"
            r"(?P<heure>\d{2}:\d{2}:\d{2},\d{3})\s+"
            r"(?P<niveau>[A-Z]+)\s+-\s*"
            r"(?P<message>.*)"
        )
        m = log_pattern.match(line)
        if m:
            return m.groupdict()
        return None


    def extract_block_metadata(self, block_dict):
        block_dict["date"] = block_dict["entries"][0]["date"]
        # Regexp
        re_source_dest = re.compile(r"^(?P<src>\w+)\s+to\s+(?P<dst>\w+)$")
        re_mapping_name = re.compile(r"^Processing mapping:\s+(?P<name>.+)$")
        re_final = re.compile(r"^Finished Processing\s+\[(?P<status>\w+)\]\s+(?P<name>.+)$")
        for entry in block_dict["entries"]:
            msg = entry["message"]

            # Source / destination
            m = re_source_dest.match(msg)
            if m:
                block_dict["source"] = m.group("src")
                block_dict["destination"] = m.group("dst")
                continue

            # Mapping name
            m = re_mapping_name.match(msg)
            if m:
                block_dict["mapping_name"] = m.group("name")
                continue

            # Final status
            m = re_final.match(msg)
            if m:
                block_dict["final_status"] = m.group("status")
                block_dict["final_name"] = m.group("name")
                continue


    def parse_log_blocks(self):
        blocks = []
        current_entries = []

        with open(self.latest_log, "r", encoding="utf-8") as f:
            for raw_line in f:
                parsed = self.parse_log_line(raw_line.strip())
                if not parsed:
                    continue

                if parsed["message"] == "":
                    # Fin d’un bloc → enregistrer avec métadonnées
                    if current_entries:
                        block = {
                            "entries": current_entries,
                            "source": None,
                            "destination": None,
                            "mapping_name": None,
                            "final_status": None,
                            "final_name": None,
                        }
                        self.extract_block_metadata(block)
                        blocks.append(block)
                        current_entries = []
                else:
                    current_entries.append(parsed)

        # Dernier bloc si le fichier ne finit pas par une ligne vide
        if current_entries:
            block = {
                "entries": current_entries,
                "source": None,
                "destination": None,
                "mapping_name": None,
                "final_status": None,
                "final_name": None,
            }
            self.extract_block_metadata(block)
            blocks.append(block)

        return blocks


    def find_log_files(self):
        pattern = self.build_filename_regex()
        log_files = []

        if not self.root_log_dir.exists():
            raise FileNotFoundError(f"Répertoire introuvable : {self.root_log_dir}")

        for f in self.root_log_dir.iterdir():
            if f.is_file() and pattern.match(f.name):
                log_files.append(f)

        return sorted(log_files)


    def find_latest_log_file(self):
        pattern = self.build_filename_regex()
        dated_files = []

        path = Path(self.root_log_dir)

        for f in path.iterdir():
            if f.is_file() and pattern.match(f.name):
                # extrait YYYY-MM-DD
                date_str = f.name.rsplit("_", 1)[1].replace(".txt", "")
                dated_files.append((f, date_str))

        if not dated_files:
            return None

        # trier par date YYYY-MM-DD (format ISO → tri correct naturellement)
        dated_files.sort(key=lambda x: x[1], reverse=True)

        # renvoyer le fichier le plus récent
        return dated_files[0][0]
