// SPDX-License-Identifier: MIT
pragma solidity 0.8.15;

// Testing utilities
import { Bridge_Initializer } from "test/setup/Bridge_Initializer.sol";
import { Reverter, ConfigurableCaller } from "test/mocks/Callers.sol";
import { EIP1967Helper } from "test/mocks/EIP1967Helper.sol";

// Libraries
import { Hashing } from "src/libraries/Hashing.sol";
import { Encoding } from "src/libraries/Encoding.sol";
import { Types } from "src/libraries/Types.sol";
import { ICrossL2Inbox, IL2ToL2CrossDomainMessenger } from "src/libraries/Predeploys.sol";

// Target contract dependencies
import { L2ToL1MessagePasser } from "src/L2/L2ToL1MessagePasser.sol";
import { AddressAliasHelper } from "src/vendor/AddressAliasHelper.sol";

contract L2ToL2CrossDomainMessengerTest is Bridge_Initializer {
    /// @dev Target address for testing
    address target = address(0xabcd);

    /// @dev Tests that the implementation is initialized correctly.
    function test_constructor_succeeds() external {
        assertEq(l2ToL2CrossDomainMessenger.MESSAGE_VERSION(), uint16(0));
        assertEq(l2ToL2CrossDomainMessenger.INITIAL_BALANCE(), type(uint248).max);
        assertEq(l2ToL2CrossDomainMessenger.crossL2Inbox(), address(crossL2Inbox));
        assertEq(l2ToL2CrossDomainMessenger.messageNonce(), uint256(0));
    }

    /// @dev Tests that `messageNonce` can be decoded correctly.
    function test_messageVersion_succeeds() external {
        assertEq(address(l2ToL2CrossDomainMessenger), 0x4200000000000000000000000000000000000023);
        (, uint16 version) = Encoding.decodeVersionedNonce(l2ToL2CrossDomainMessenger.messageNonce());
        assertEq(version, l2ToL2CrossDomainMessenger.MESSAGE_VERSION());
    }

    /// @dev Tests that `sendMessage` executes successfully.
    function testFuzz_sendMessage_succeeds(uint256 _destination, address _target) external {
        vm.assume(_destination != block.chainid);

        bytes memory xDomainCallData = hex"aa";

        l2ToL2CrossDomainMessenger.sendMessage(_destination, _target, xDomainCallData);
    }

    /// @dev Tests that `sendMessage` can be called twice and that
    ///      the nonce increments correctly.
    function test_sendMessage_twice_succeeds() external {
        uint256 destination = 123 == block.chainid ? 456 : 123;
        bytes memory xDomainCallData = hex"aa";
        uint256 nonce = l2ToL2CrossDomainMessenger.messageNonce();
        l2ToL2CrossDomainMessenger.sendMessage(destination, target, xDomainCallData);
        l2ToL2CrossDomainMessenger.sendMessage(destination, target, xDomainCallData);
        // the nonce increments for each message sent
        assertEq(nonce + 2, l2ToL2CrossDomainMessenger.messageNonce());
    }

    /// @dev Tests that `relayMessage` executes successfully.
    function test_relayMessage_succeeds() external {
        address sender = address(l2ToL2CrossDomainMessenger);

        ICrossL2Inbox.Identifier memory id = ICrossL2Inbox.Identifier({
            origin: address(l2ToL2CrossDomainMessenger),
            blocknumber: 0,
            logIndex: 0,
            timestamp: block.timestamp,
            chainId: block.chainid
        });

        vm.prank(tx.origin);
        crossL2Inbox.executeMessage(id, target, hex"1111");

        vm.prank(address(crossL2Inbox));
        l2ToL2CrossDomainMessenger.relayMessage(
            block.chainid,
            Encoding.encodeVersionedNonce(0, 1), // nonce
            sender,
            target,
            0, // value
            hex"1111"
        );

        assert(
            l2ToL2CrossDomainMessenger.successfulMessages(
                keccak256(abi.encode(block.chainid, Encoding.encodeVersionedNonce(0, 1), sender, target, 0, hex"1111"))
            )
        );
    }

    /// @dev Tests that `relayMessage` reverts if attempting to relay
    ///      a message sent to an L1 system contract.
    function test_relayMessage_toSystemContract_reverts() external {
        address sender = address(l1CrossDomainMessenger);
        address caller = AddressAliasHelper.applyL1ToL2Alias(address(l1CrossDomainMessenger));
        bytes memory message = hex"1111";

        vm.prank(caller);
        vm.expectRevert("CrossDomainMessenger: message cannot be replayed");
        l1CrossDomainMessenger.relayMessage(Encoding.encodeVersionedNonce(0, 1), sender, target, 0, 0, message);
    }

    /// @dev Tests that `relayMessage` is able to send a successful call
    ///      to the target contract after the first message fails and ETH
    ///      gets stuck, but the second message succeeds.
    function test_relayMessage_retry_succeeds() external {
        address sender = address(crossL2Inbox);
        uint256 value = 100;

        bytes32 hash =
            keccak256(abi.encode(block.chainid, Encoding.encodeVersionedNonce(0, 1), sender, target, value, hex"1111"));

        ICrossL2Inbox.Identifier memory id = ICrossL2Inbox.Identifier({
            origin: address(l2ToL2CrossDomainMessenger),
            blocknumber: 0,
            logIndex: 0,
            timestamp: block.timestamp,
            chainId: block.chainid
        });

        vm.prank(tx.origin);
        crossL2Inbox.executeMessage(id, target, hex"1111");

        vm.etch(target, address(new Reverter()).code);
        vm.deal(address(crossL2Inbox), value);
        vm.prank(address(crossL2Inbox));
        l2ToL2CrossDomainMessenger.relayMessage{ value: value }(
            block.chainid,
            Encoding.encodeVersionedNonce(0, 1), // nonce
            sender,
            target,
            value,
            hex"1111"
        );

        assertEq(address(l2ToL2CrossDomainMessenger).balance, value);
        assertEq(address(target).balance, 0);
        assertEq(l2ToL2CrossDomainMessenger.successfulMessages(hash), false);
        //assertEq(l2ToL2CrossDomainMessenger.failedMessages(hash), true);

        vm.expectEmit(true, true, true, true);

        emit RelayedMessage(hash);

        vm.etch(target, address(0).code);
        vm.prank(address(sender));
        l2ToL2CrossDomainMessenger.relayMessage(
            block.chainid,
            Encoding.encodeVersionedNonce(0, 1), // nonce
            sender,
            target,
            value,
            hex"1111"
        );

        assertEq(address(l2ToL2CrossDomainMessenger).balance, 0);
        assertEq(address(target).balance, value);
        assertEq(l2ToL2CrossDomainMessenger.successfulMessages(hash), true);
        //assertEq(l2ToL2CrossDomainMessenger.failedMessages(hash), true);
    }
}