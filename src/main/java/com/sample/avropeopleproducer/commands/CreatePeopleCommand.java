package com.sample.avropeopleproducer.commands;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class CreatePeopleCommand {
    private int count;
}
