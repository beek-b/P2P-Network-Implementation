#[derive(Debug)]
pub enum SuccessTaskRead {
    Successful,

    SuccessfulEOF,
}

#[derive(Debug)]
pub enum SuccessTaskWrite {
    Successful,

    
}

#[derive(Debug)]
pub enum SuccessPeer {
    SuccessfulTaskReadTaskWrite,
    SuccessfulTaskReadBrokenPipeTaskWriteSuccessful,
}