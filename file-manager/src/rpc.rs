use std::convert::TryFrom;
pub enum RequestCode {
    Query = 1,
    Echo = 42,
}

impl TryFrom<u8> for RequestCode {
    type Error = ();

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == RequestCode::Query as u8 => Ok(RequestCode::Query),
            x if x == RequestCode::Echo as u8 => Ok(RequestCode::Echo),
            _ => Err(()),
        }
    }
}


tonic::include_proto!("ebi_rpc");
