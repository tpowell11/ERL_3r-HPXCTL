use std::vec;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::detector::DetectorInfo;
#[derive(Serialize,Deserialize, Debug)]
pub enum BitDepth {
    D8=8,
    D16=16,
    D32=32,
}
#[derive(Serialize,Deserialize, Debug)]
pub struct Image {
    pub id:Uuid,
    pub width:usize,
    pub height:usize,
    pub data:Vec<u8>,
    bit_depth:BitDepth,
    detector_info: DetectorInfo,
}
impl Image {
    pub fn new() -> Self {
        Self {
            id: Uuid::nil(),
            width: 0,
            height: 0, 
            data: Vec::new(),
            bit_depth: BitDepth::D16,
            detector_info: DetectorInfo::new()
        }
    }
    pub fn push_data<T:Into<u8> + Into<u16> + Into<u32>>(& mut self, incoming: Vec<T>) {
        match self.bit_depth {
            BitDepth::D8 => {
                for pixel in incoming {
                    self.data.push(Into::<u8>::into(pixel));
                }
            }
            BitDepth::D16 => {
                let mut bytes:[u8;2] = [0,0];
                for pixel in incoming {
                    let pixu16 = Into::<u16>::into(pixel);
                    bytes[0] = pixu16 as u8;
                    bytes[1] = (pixu16 >> 8) as u8;
                    self.data.append(& mut bytes.to_vec());
                }
            }
            BitDepth::D32 => {
                todo!()
            }
        }
    }
    pub fn uuid_match(&self, other: Uuid) -> bool {
        return self.id == other;
    }
    pub fn set_rows(&mut self, rows: usize) -> () {
        self.height = rows
    }
    pub fn set_cols(&mut self, cols: usize) -> () {
        self.width = cols
    }
}
// ! This should get axed.
impl Default for Image {
    fn default() -> Self {
        Self { id: Default::default(), 
            width: Default::default(), 
            height: Default::default(), 
            data: Default::default(),
            bit_depth: BitDepth::D16,
            detector_info: DetectorInfo::new()
        }
    }
}