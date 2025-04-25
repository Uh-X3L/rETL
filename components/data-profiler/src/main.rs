use clap::Parser;
mod profiler;
use std::path::Path;

#[derive(Parser, Debug)]
struct Cli {
    #[arg(long)]
    file: String,

    #[arg(long)]
    output: Option<String>,

    #[arg(long, default_value_t = b',' as u8)]
    delimiter: u8,
}

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    let path = std::path::Path::new(&args.file);

    let df = profiler::read_dataframe(path, &profiler::infer_format(path), args.delimiter)?;
    let (rows, profiles) = profiler::profile_df_detailed(&df)?;

    println!("✅ Rows: {rows}");

    if let Some(out_path) = args.output {
        profiler::export_profile_to_json(&profiles, Path::new(&out_path))?;
        println!("📦 Profile written to {out_path}");
    } else {
        for p in profiles {
            println!("{:#?}", p);
        }
    }

    Ok(())
}

