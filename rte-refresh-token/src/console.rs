use crate::cmd;

pub fn exec(token: String) -> cmd::Result<()> {
    println!("{}", token);
    Ok(())
}
