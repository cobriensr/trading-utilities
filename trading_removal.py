"""Remove Files After Being Processed (macOS Compatible)"""
import os
from send2trash import send2trash

# pylint: disable=missing-function-docstring, missing-final-newline, trailing-whitespace

FOLDER_PATH = "/Users/charlesobrien/Desktop/Tradingview-Files"


def is_visible_file(filename):
    return not filename.startswith(".") and filename != ".DS_Store"


def process_folder(folder_path):
    try:
        # Use folder_path instead of FOLDER_PATH
        all_files = os.listdir(folder_path)
        visible_files = [f for f in all_files if is_visible_file(f)]

        if not visible_files:
            print(f"No visible files found in the folder: {folder_path}")
        elif len(visible_files) > 1:
            print(f"Multiple visible files found in the folder: {folder_path}")
            for file in visible_files:
                print(f"- {file}")
        else:
            file_path = os.path.join(folder_path, visible_files[0])
            send2trash(file_path)
            print(f"File '{file_path}' has been moved to the trash.")
    except FileNotFoundError:
        print(f"The specified folder does not exist: {folder_path}")
    except OSError as e:
        print(f"An error occurred while accessing the file or folder: {str(e)}")


def main():
    for folder in os.listdir(FOLDER_PATH):
        subfolder_path = os.path.join(FOLDER_PATH, folder)
        if os.path.isdir(subfolder_path):
            print(f"Processing folder: {subfolder_path}")
            process_folder(subfolder_path)


if __name__ == "__main__":
    main()
