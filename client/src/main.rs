use std::io::{self, Write};

fn main() {
    println!("Hello, world!");
    let mut line = String::new();
    loop {
        print!(">>> ");
        io::stdout().flush().unwrap();
        line.clear();
        io::stdin()
            .read_line(&mut line)
            .expect("Failed to read line");

        // Remove the newline character at the end (added by read_line)
        line.truncate(line.trim_end().len());

        println!("Hello, {}!", line);
    }
}
