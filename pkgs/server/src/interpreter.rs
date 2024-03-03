use uuid::Uuid;

pub mod runtime {
    use uuid::Uuid;

    pub struct Runtime {
        pub current_state: State,
    }


    impl Runtime {
        pub fn new() -> Runtime {
            Runtime { current_state: State::new() }
        }

        pub fn execute(input: String){

        }
    }

    pub struct State {
        space: &'static str,
        pub connection_id: Uuid,
    }

    impl State {
        fn new() -> State {
            State {
                space: "default",
                connection_id: Uuid::new_v4(),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use crate::interpreter::runtime::Runtime;

        #[test]
        fn runtime_init() {
            let runtime = Runtime::new();
        }
    }

}


mod lexcal {
    use logos::Logos;

    #[derive(Logos, Debug, PartialEq)]
    #[logos(skip r"[ \t\n\f]+")] // Ignore this regex pattern between tokens
    enum Token {
        #[token("SET SPACE")]
        SetSpace,
        #[token("RESERVE")]
        Reserve,
        #[token("CONFIRM")]
        Confirm,
        #[token("PUSH EVENT")]
        PushEvent,
        #[token("PULL EVENTS")]
        PullEvents,
        #[token("DELETE EVENT")]
        DeleteEvent,
        #[token("DELETE STREAM")]
        DeleteStream,
        #[token("PUSH PROJECTION")]
        PushProjection,
        #[token("PULL PROJECTION")]
        PullProjection,
        #[token("PULL PROJECTIONS")]
        PullProjections,
        #[token("(")]
        OpenBracket,
        #[token(")")]
        CloseBracket,
        #[token(";")]
        Semicolon,
        #[regex("[a-zA-Z]+")]
        Text,
    }
}
