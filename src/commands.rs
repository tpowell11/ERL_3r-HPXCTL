use std::collections::VecDeque;
/// Container for the text and result of a command.
/// Commanmd has been resolved if `Command.result` is Some().
/// User is responsible for errror handling.
#[derive(Debug)]
pub struct Command {
    text: String,
    pub result: Option<String>,
}
impl Command {
    pub fn new(cmd: &'static str, args: &dyn ToString) -> Self {
        Self {
            text: format!("{} {}", cmd.to_string(), args.to_string()),
            result: None,
        }
    }
    pub fn from_arg_array(cmd: &'static str, args: Vec<&dyn ToString>) -> Self {
        let mut arg_string = String::new();
        for arg in args {
            arg_string.push(' ');
            arg_string.push_str(arg.to_string().as_str());
        }
        return Self {
            text: format!("{cmd}{arg_string}"),
            result: None,
        };
    }
    pub fn text(&self) -> String {
        return self.text.clone();
    }
    pub fn vals(&self) -> Option<VecDeque<String>> {
        match &self.result {
            Some(s) => {
                let ss = s.split(' ').collect::<VecDeque<&str>>();
                let mut sss = ss
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<VecDeque<String>>();
                let _ = sss.pop_front();
                return Some(sss);
            }
            None => {
                return None;
            }
        }
    }
}

