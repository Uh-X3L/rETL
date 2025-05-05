use clap::Parser;
mod cli;
mod pipeline;

use cli::Cli;

fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    pipeline::run_pipeline(
        args.input.file.to_str().unwrap(),
        &format!("{:?}", args.input.format).to_lowercase(),
        args.output.output.as_ref().map(|p| p.to_str().unwrap()),
        &format!("{:?}", args.output.out_format).to_lowercase(),
        args.transform.filter_col.as_deref(),
        args.transform.filter_val.as_deref(),
        args.transform.drop_nulls,
    )
}
