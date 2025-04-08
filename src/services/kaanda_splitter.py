import os
import re


def split_kaanda_file(input_file, output_dir):
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Read the input file
    with open(input_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Regular expressions
    shloka_pattern = re.compile(r"(\d+)-(\d+)-(\d+)")
    opening_tag_pattern = re.compile(r"^<([^/][^>]*)>")

    current_adhyaya = None
    adhyaya_start_idx = 0  # Always start the first adhyaya at the first line
    highest_shloka_idx = 0
    highest_shloka_num = 0
    i = 0

    while i < len(lines):
        line = lines[i]

        # Check if this line contains a shloka number
        match = shloka_pattern.search(line)
        if match:
            kaanda_num, adhyaya_num, shloka_num = map(int, match.groups())

            # If this is the first adhyaya we're seeing
            if current_adhyaya is None:
                current_adhyaya = adhyaya_num
                # No need to set adhyaya_start_idx as it's already 0
                highest_shloka_num = shloka_num
                highest_shloka_idx = i

            # If we've found a new adhyaya
            elif adhyaya_num != current_adhyaya:
                # Go back to the line with highest shloka number
                j = highest_shloka_idx

                # Look for the next line with an opening tag
                while j < len(lines) - 1:
                    j += 1
                    if opening_tag_pattern.match(lines[j].strip()):
                        break

                # The line before the opening tag is the end of the current adhyaya
                adhyaya_end_idx = j - 1

                # Save the current adhyaya
                adhyaya_content = lines[adhyaya_start_idx : adhyaya_end_idx + 1]
                output_file = os.path.join(output_dir, f"{current_adhyaya}.txt")
                with open(output_file, "w", encoding="utf-8") as f:
                    f.writelines(adhyaya_content)
                print(
                    f"Created adhyaya {current_adhyaya} with {len(adhyaya_content)} lines"
                )

                # Start the next adhyaya from the line after the end of the previous one
                # This ensures opening tags are included in the next adhyaya
                adhyaya_start_idx = adhyaya_end_idx + 1
                current_adhyaya = adhyaya_num
                highest_shloka_num = shloka_num
                highest_shloka_idx = i

            # Update highest shloka number for current adhyaya
            elif shloka_num > highest_shloka_num:
                highest_shloka_num = shloka_num
                highest_shloka_idx = i

        i += 1

    # Handle the last adhyaya
    if current_adhyaya is not None:
        adhyaya_content = lines[adhyaya_start_idx:]
        output_file = os.path.join(output_dir, f"{current_adhyaya}.txt")
        with open(output_file, "w", encoding="utf-8") as f:
            f.writelines(adhyaya_content)
        print(f"Created adhyaya {current_adhyaya} with {len(adhyaya_content)} lines")


if __name__ == "__main__":
    input_files = [
        "test_files/1-बालकाण्डम्.txt",
        "test_files/2-अयोध्याकाण्डम्.txt",
        "test_files/3-अरण्यकाण्डम्.txt",
        "test_files/4-किष्किन्धाकाण्डम्.txt",
        "test_files/5-सुन्दरकाण्डम्.txt",
        "test_files/6-युद्धकाण्डम्.txt",
        "test_files/7-उत्तरकाण्डम्.txt",
    ]

    output_dirs = [
        "ramayana/1_बालकाण्डम्/",
        "ramayana/2_अयोध्याकाण्डम्/",
        "ramayana/3_अरण्यकाण्डम्/",
        "ramayana/4_किष्किन्धाकाण्डम्/",
        "ramayana/5_सुन्दरकाण्डम्/",
        "ramayana/6_युद्धकाण्डम्/",
        "ramayana/7_उत्तरकाण्डम्/",
    ]

    for input_file, output_dir in zip(input_files, output_dirs):
        split_kaanda_file(input_file, output_dir)
