import os
import re
import io

import pandas as pd


def process_all(file_dir, file_target, curr_dir):

    # Definitions Setup
    # OLD DATASETS SIGS only 2021
    df_old = pd.read_csv(f"{curr_dir}/INPUTs/regimens_legacy.tsv", sep='\t')
    df = pd.read_csv(file_target, sep='\t')
    regname_new = set(df['regName'].unique())
    regname_old = set(df_old['regName'].unique())
    regname_new_mapping = {}
    regname_old_mapping = {}

    # Procesing - Syncs
    regname_old = set([x.replace("&", "and") for x in regname_old])
    regname_new = set([x.lower() for x in regname_new])
    regname_old = set([x.lower() for x in regname_old])
    regname_old = set([x if x != "carfilzomib" else x + " monotherapy" for x in regname_old])
    regname_new = set([x.replace("(tp)", "").strip() for x in regname_new])
    regname_new = set([x.replace("(kr)", "").strip() for x in regname_new])
    # removing all and and & to ","
    regname_new = set([x.replace(" and ", ",").strip() for x in regname_new])
    regname_new = set([",".join([y.strip() for y in x.split(",")]) for x in regname_new])
    regname_old = set([x.replace(" and ", ",").strip() for x in regname_old])
    regname_old = set([",".join([y.strip() for y in x.split(",")]) for x in regname_old])
    # Supplementary wrong
    # Case 1
    # old - bortezomib, thalidomide and panobinostat
    # new - 'bortezomib, thalidomide, <<dexamethasone>>, panobinostat'
    regname_new = {
        x.replace("dexamethasone,", "") if re.search(r'(?=.*bortezomib)(?=.*thalidomide)', x, re.IGNORECASE)
        else x
        for x in regname_new
    }
    # Case 2
    # old - melphalan flufenamide monotherapy'
    # new - 'melphalan flufenamide,dexamethasone'
    regname_new = {
        x.replace(",dexamethasone", " monotherapy") if re.search(r'(?=.*melphalan)(?=.*flufenamide)', x, re.IGNORECASE)
        else x
        for x in regname_new
    }
    # case 3
    # old - tegafur,uracil monotherapy
    # new -'tegafur,uracil,folinic acid'
    regname_new = {
        x.replace(",folinic acid", " monotherapy") if re.search(r'(?=.*tegafur)(?=.*uracil)', x, re.IGNORECASE)
        else x
        for x in regname_new
    }
    # case 4
    # old - 'vmp + rd',
    #       'vmp / rd'
    # new - vmp,then rd, vmp/rd
    regname_new = {
        x.replace(",then rd", " + rd") if re.search(r'(?=.*vmp)(?=.*then rd)', x, re.IGNORECASE)
        else x
        for x in regname_new
    }
    regname_new = {
        x.replace("/", " / ") if re.search(r'(?=.*vmp)(?=.*/rd)', x, re.IGNORECASE)
        else x
        for x in regname_new
    }

    # Mapping
    for name in df['regName'].unique():
        original = name
        transformed = name.lower().replace("(tp)", "").replace("(kr)", "").strip()
        transformed = transformed.replace(" and ", ",")
        transformed = ",".join([x.strip() for x in transformed.split(",")])

        # Case-specific adjustments
        if re.search(r'(?=.*bortezomib)(?=.*thalidomide)', transformed):
            transformed = transformed.replace("dexamethasone,", "")
        if re.search(r'(?=.*melphalan)(?=.*flufenamide)', transformed):
            transformed = transformed.replace(",dexamethasone", " monotherapy")
        if re.search(r'(?=.*tegafur)(?=.*uracil)', transformed):
            transformed = transformed.replace(",folinic acid", " monotherapy")
        if re.search(r'(?=.*vmp)(?=.*then rd)', transformed):
            transformed = transformed.replace(",then rd", " + rd")
        if re.search(r'(?=.*vmp)(?=.*/rd)', transformed):
            transformed = transformed.replace("/", " / ")

        regname_new_mapping[transformed] = original

    for name in df_old['regName'].unique():
        original = name

        transformed = name.lower().replace("(tp)", "").replace("(kr)", "").strip()
        transformed = name.replace("&", "and")
        transformed = transformed.lower()
        if transformed == "carfilzomib":
            transformed = transformed + " monotherapy"
        transformed = transformed.replace(" and ", ",")
        transformed = ",".join([x.strip() for x in transformed.split(",")])

        regname_old_mapping[transformed] = original

    # Definitions Shared
    shared = regname_new & regname_old

    # 2. Inner Match Summary (regString for intersecting regNames)
    match = 0
    mismatch = 0
    for name in shared:
        old_str = df_old[df_old['regName'] == name]['regString'].unique()
        new_str = df[df['regName'] == name]['regString'].unique()
        if len(old_str) == 1 and len(new_str) == 1 and old_str[0] == new_str[0]:
            match += 1
        else:
            mismatch += 1

    new_shared_mapping = {k: V for k, V in regname_new_mapping.items() if k in shared}
    old__shared_mapping = {k: V for k, V in regname_old_mapping.items() if k in shared}

    df_shared = df[df.regName.isin(new_shared_mapping.values())]

    return df_shared, df, new_shared_mapping, df_old, old__shared_mapping


def markdown_table(file_path, df_shared, df, new_shared_mapping, df_old, old__shared_mapping):
    output_buffer = io.StringIO()
    correct_count = 0
    warning_count = 0
    failed_count = 0
    reverse_new_shared_mapping = {v: k for k, v in new_shared_mapping.items()}

    sections = []  # (priority, regimen, block)

    for i, group in df_shared.groupby("regName"):
        ss_new = df[df.regName == i].shortString.unique().tolist()
        _shared_value = reverse_new_shared_mapping[i]
        _old_value = old__shared_mapping[_shared_value]
        ss_old = df_old[df_old.regName == _old_value].shortString.unique().tolist()

        ss_new_clean = [s.rstrip(';').lower() for s in ss_new]
        ss_old_clean = [s.rstrip(';').lower() for s in ss_old]

        is_corrects = [s in ss_old_clean for s in ss_new_clean]
        match_status = "✅" if all(is_corrects) else "⚠️" if any(is_corrects) else "❌"

        if all(is_corrects):
            correct_count += 1
            continue  # skip writing corrects
        elif any(is_corrects):
            warning_count += 1
            priority = 1
        else:
            failed_count += 1
            priority = 2

        block = io.StringIO()
        block.write(f"=== Regimen: {i} ===\n")
        block.write(f"=== Status: {match_status} ===\n")

        block.write("new\n")
        for e, ss in enumerate(ss_new_clean):
            block.write(f"{e} {ss}\n")

        block.write("old\n")
        for e, ss in enumerate(ss_old_clean):
            block.write(f"{e} {ss}\n")

        block.write("\n")
        block.write("regimen_cui | component | variant | condition\n")
        block.write("=" * 80 + "\n")

        last_variant = None
        for _, row in group.iterrows():
            if row['variant'] != last_variant:
                if last_variant is not None:
                    block.write("-" * 80 + "\n")
                last_variant = row['variant']
            block.write(f"{int(row['regCode'])} | {row['component']} | {row['variant']} | {row['condition']}\n")

        block.write("\n")
        sections.append((priority, i, block.getvalue()))

    # Final summary at top
    summary = io.StringIO()
    summary.write("### MATCH SUMMARY ###\n")
    summary.write(f"✅ Correct: {correct_count}\n")
    summary.write(f"⚠️ Partially correct: {warning_count}\n")
    summary.write(f"❌ Failed: {failed_count}\n")
    summary.write("=" * 80 + "\n\n")

    # Sort and write sections
    sections.sort(key=lambda x: (x[0], x[1]))  # sort by priority then name
    output_buffer.write(summary.getvalue())
    for _, _, block in sections:
        output_buffer.write(block)

    # Write to file
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(output_buffer.getvalue())


def run(file_dir: str, file_target_path: str) -> None:
    """Callable entry-point used by the Prefect task and tests."""
    curr_dir = os.path.abspath(os.curdir)
    out      = os.path.abspath(f"{file_dir}/validation")
    out_file = "shared_output_analysis.txt"
    os.makedirs(out, exist_ok=True)

    df_shared, df, new_shared_mapping, df_old, old__shared_mapping = process_all(
        file_dir, file_target_path, curr_dir
    )
    markdown_table(
        out + "/" + out_file,
        df_shared, df, new_shared_mapping, df_old, old__shared_mapping,
    )
