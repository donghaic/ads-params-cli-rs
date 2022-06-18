use std::fmt;
use std::fs::File;
use std::io::{prelude::*, BufReader};

use anyhow::{anyhow, Ok, Result};
use clap::{ArgEnum, Parser, Subcommand};
use redis::Commands;
use url::Url;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    /// Redis address.
    #[clap(long, default_value_t = String::from("127.0.0.1:6379"))]
    redis_addr: String,

    /// Redis password.
    #[clap(long, default_value_t = String::from(""))]
    redis_pwd: String,

    /// Target filename to be loaded.
    #[clap(long, short, forbid_empty_values = true)]
    file: String,

    /// feishu url
    #[clap(long, validator = validate_url)]
    feishu_url: Option<String>,
}


fn validate_url(url: &str) -> std::result::Result<(), String> {
    match Url::parse(url) {
        std::result::Result::Ok(url) => std::result::Result::Ok(()),
        std::result::Result::Err(_) => std::result::Result::Err("bad url".into()),
    }
}

#[derive(Subcommand, Debug)]
enum Command {
    /// AB 参数配置
    AbParams {
        /// AB文档类型
        #[clap(arg_enum, long = "type", short = 't')]
        types: AbType,
    },

    /// 模型动作目标CTR数组下标
    ActionChoice,

    /// 投放ID模型打分
    ActionScore,

    /// 模型动作目标CTR
    ActionValue,

    ///  区间信息参数配置
    RangeSignal {
        /// 信号文件类型
        #[clap(arg_enum, long = "type", short = 't')]
        types: SignalType,
    },
}

#[derive(ArgEnum, Clone, Copy, Debug)]
enum AbType {
    Fill,
    Show,
    Click,
}

impl fmt::Display for AbType {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "{}", format!("{:?}", self).to_lowercase())
    }
}

#[derive(ArgEnum, Clone, Copy, Debug)]
enum SignalType {
    TemptClick,
    FillRate,
    ShowRate,
    ClickRate,
}

const REDIS_CFG_KEY_EXP_EXP_AB_PARAMS: &str = "cfg:exp:ab";
const REDIS_KEY_EXP_ADID_DEFALUT_CHOICE: &str = "exp:default:adid:choices";
const REDIS_CFG_KEY_EXP_TARGET_CTR_ACTION: &str = "cfg:exp:action:targetctr:default";
const REDIS_CFG_KEY_EXP_VERSION_AD_ID_SCORES: &str = "expversion:score:default";

fn main() -> Result<()> {
    let cli = Cli::parse();

    println!("command: {:?}", cli.command);
    println!("redis_addr: {:?}", cli.redis_addr);
    println!("file: {:?}", cli.file);

    match cli.command {
        Command::AbParams { types } => {
            println!("AbParams types: {:?}", types);
            handle_ab_params(&cli, types)
        }
        Command::ActionChoice => {
            println!("ActionChoice");
            handle_action_choice(&cli)
        }
        Command::ActionScore => {
            println!("ActionScore");
            handle_action_score(&cli)
        }
        Command::ActionValue => {
            println!("ActionValue");
            handle_action_value(&cli)
        }
        Command::RangeSignal { types } => {
            println!("types: {:?}", types);
            handle_range_signal(&cli, types)
        }
    }
}

fn handle_ab_params(cli: &Cli, types: AbType) -> Result<()> {
    let client = redis::Client::open(format!("redis://{}", cli.redis_addr))?;
    let mut con = client.get_connection()?;

    let file = File::open(&format!("{}", cli.file))?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let kv = tuple2_from_split(&line.as_str(), '=', "bad line");
        if kv.is_ok() {
            let kv = kv.unwrap();
            con.hset(
                REDIS_CFG_KEY_EXP_EXP_AB_PARAMS,
                format!("{}:{}", kv.0, types),
                kv.1,
            )?;
        }
    }

    Ok(())
}

fn handle_action_choice(cli: &Cli) -> Result<()> {
    let client = redis::Client::open(format!("redis://{}", cli.redis_addr))?;
    let mut con = client.get_connection()?;

    let file = File::open(&format!("{}", cli.file))?;
    let reader = BufReader::new(file);

    let mut items = vec![];

    for line in reader.lines() {
        let line = line?;
        let kv = tuple2_from_split(&line.as_str(), '=', "bad line")?;
        // con.hset(
        //     RedisKey_ExpAdidDefalutChoice,
        //     format!("{}:{}", kv.0, kv.1),
        //     kv.1,
        // )?;
        items.push(kv);
    }
    if !items.is_empty() {
        con.hset_multiple(REDIS_KEY_EXP_ADID_DEFALUT_CHOICE, &items)?;
    }

    Ok(())
}

fn handle_action_score(cli: &Cli) -> Result<()> {
    let client = redis::Client::open(format!("redis://{}", cli.redis_addr))?;
    let mut con = client.get_connection()?;

    let file = File::open(&format!("{}", cli.file))?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let kv = tuple2_from_split(&line.as_str(), '=', "bad line")?;
        let values: Vec<f32> = serde_json::from_str(&kv.1)?;
        let mut items = vec![];

        for (action_id, val) in values.iter().enumerate() {
            items.push((format!("{}", action_id), format!("{}", val)))
        }
        con.hset_multiple(
            format!("{}:{}", REDIS_CFG_KEY_EXP_VERSION_AD_ID_SCORES, kv.0),
            &items,
        )?;
    }
    Ok(())
}

fn handle_action_value(cli: &Cli) -> Result<()> {
    let client = redis::Client::open(format!("redis://{}", cli.redis_addr))?;
    let mut con = client.get_connection()?;

    let mut file = File::open(&format!("{}", cli.file))?;

    let mut data = String::new();
    file.read_to_string(&mut data)?;
    let kv = tuple2_from_split(&data, '=', "bad line")?;
    let values: Vec<f32> = serde_json::from_str(&kv.1)?;

    for (action_id, val) in values.iter().enumerate() {
        con.hset(
            REDIS_CFG_KEY_EXP_TARGET_CTR_ACTION,
            format!("{}", action_id),
            format!("{}", val),
        )?;
    }

    Ok(())
}

fn handle_range_signal(cli: &Cli, types: SignalType) -> Result<()> {
    todo!();
}

fn tuple2_from_split<'a>(value: &'a str, pat: char, msg: &'static str) -> Result<(String, String)> {
    let mut split = value.split(pat);
    let v1 = split.next().ok_or_else(|| anyhow!(msg))?.to_owned();
    let v2 = split.next().ok_or_else(|| anyhow!(msg))?.to_owned();
    if split.next().is_some() {
        return Err(anyhow!(msg));
    }

    Ok((v1, v2))
}
