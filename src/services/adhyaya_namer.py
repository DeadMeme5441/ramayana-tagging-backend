import os
import json


def get_files_last_lines(directory_path):
    # Dictionary to store results
    last_lines_dict = {1: ""}

    # Get all files in the directory
    try:
        files = [
            f
            for f in os.listdir(directory_path)
            if os.path.isfile(os.path.join(directory_path, f))
        ]

        files = sorted(files, key=lambda x: int(x.split(".")[0]))

    except FileNotFoundError:
        print(f"Error: Directory {directory_path} not found")
        return {}

    # Process each file
    for i, file_name in enumerate(files):
        file_path = os.path.join(directory_path, str(file_name))

        try:
            # Read the last line of the file
            with open(file_path, "r", encoding="utf-8") as f:
                # Skip to the end of the file
                try:
                    f.seek(0, os.SEEK_END)
                    pos = f.tell() - 1

                    # Read backwards until the start of the file or a newline character
                    while pos > 0 and f.read(1) != "\n":
                        pos -= 1
                        f.seek(pos, os.SEEK_SET)

                    # Read the last line
                    last_line = f.readline().strip()

                    # If file is empty or only has one line
                    if not last_line and pos == 0:
                        f.seek(0)
                        last_line = f.readline().strip()
                except:
                    # Fallback if seeking doesn't work (e.g., for text files with different line endings)
                    f.seek(0)
                    lines = f.readlines()
                    last_line = lines[-1].strip() if lines else ""

            # Add to dictionary with incremented index
            last_lines_dict[i + 2] = last_line

        except Exception as e:
            print(f"Error reading file {file_name}: {e}")
            last_lines_dict[i + 2] = f"Error: {e}"

    return last_lines_dict


def delete_last_line_from_files(directory_path):
    """
    Delete the last line from each file in the specified directory
    and save the modified file.

    Args:
        directory_path: Path to the directory containing files

    Returns:
        dict: A dictionary with filenames as keys and status messages as values
    """
    result = {}

    # Get all files in the directory
    try:
        files = [
            f
            for f in os.listdir(directory_path)
            if os.path.isfile(os.path.join(directory_path, f))
        ]
    except FileNotFoundError:
        print(f"Error: Directory {directory_path} not found")
        return {"error": f"Directory {directory_path} not found"}

    # Process each file
    for file_name in files:
        file_path = os.path.join(directory_path, file_name)

        try:
            # Read all lines from the file
            with open(file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # If file is not empty, remove the last line
            if lines:
                # Write all lines except the last one back to the file
                with open(file_path, "w", encoding="utf-8") as f:
                    f.writelines(lines[:-1])
                result[file_name] = "Successfully deleted last line"
            else:
                result[file_name] = "File is empty, nothing to delete"

        except Exception as e:
            print(f"Error processing file {file_name}: {e}")
            result[file_name] = f"Error: {e}"

    return result


if __name__ == "__main__":
    # Directory to process - change this to your target directory
    for directory_to_process in [
        "ramayana/2_अयोध्याकाण्डम्/",
        "ramayana/3_अरण्यकाण्डम्/",
        "ramayana/4_किष्किन्धाकाण्डम्/",
        "ramayana/5_सुन्दरकाण्डम्/",
        "ramayana/6_युद्धकाण्डम्/",
        "ramayana/7_उत्तरकाण्डम्/",
    ]:

        delete_last_line_from_files(directory_to_process)

        # # Get and print the dictionary
        # result = get_files_last_lines(directory_to_process)

        # with open("adhyaaya_names.json", "w", encoding="utf-8") as f:
        #     json.dump(result, f, ensure_ascii=False, indent=4)
