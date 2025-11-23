// File: src/spl_system_decoder.rs

use std::str::FromStr;

use anyhow::{Result, anyhow};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    instruction::{CompiledInstruction, Instruction},
    native_token::LAMPORTS_PER_SOL,
    pubkey::Pubkey,
    system_instruction,
    transaction::VersionedTransaction,
};
use solana_transaction_status::{
    UiCompiledInstruction, UiTransactionStatusMeta, option_serializer::OptionSerializer,
};
use spl_token::instruction::{AuthorityType, TokenInstruction};

#[derive(Debug, PartialEq)]
pub enum DecodedInstruction {
    SystemTransfer {
        source: Pubkey,
        destination: Pubkey,
        lamports: u64,
    },
    SplInitializeMint {
        mint: Pubkey,
        mint_authority: Pubkey,
        freeze_authority: Option<Pubkey>,
        decimals: u8,
    },
    SplInitializeAccount {
        account: Pubkey,
        mint: Pubkey,
        owner: Pubkey,
    },
    SplInitializeMultisig {
        multisig: Pubkey,
        signers: Vec<Pubkey>,
        m: u8, // The number of required signers.
    },
    SplTokenTransfer {
        source: Pubkey,
        destination: Pubkey,
        authority: Pubkey,
        amount: u64,
    },
    SplTokenTransferChecked {
        source: Pubkey,
        destination: Pubkey,
        authority: Pubkey,
        amount: u64,
        mint: Pubkey,
        decimals: u8,
    },
    SplApprove {
        source: Pubkey,
        delegate: Pubkey,
        owner: Pubkey,
        amount: u64,
        decimals: Option<u8>,
    },
    SplRevoke {
        source: Pubkey,
        owner: Pubkey,
    },
    SplSetAuthority {
        account_or_mint: Pubkey,
        current_authority: Pubkey,
        new_authority: Option<Pubkey>,
        authority_type: AuthorityType,
    },
    SplMintTo {
        mint: Pubkey,
        destination_account: Pubkey,
        authority: Pubkey,
        amount: u64,
    },
    SplTokenBurn {
        mint: Pubkey,
        source_account: Pubkey,
        authority: Pubkey,
        amount: u64,
    },
    SplCloseAccount {
        account_to_close: Pubkey,
        destination: Pubkey,
        owner: Pubkey,
    },
    SplFreezeAccount {
        account_to_freeze: Pubkey,
        mint: Pubkey,
        authority: Pubkey,
    },
    SplThawAccount {
        account_to_thaw: Pubkey,
        mint: Pubkey,
        authority: Pubkey,
    },
    SplSyncNative {
        account: Pubkey,
    },
    ComputeBudgetSetUnitPrice {
        micro_lamports: u64,
    },
}

/// Decodes a single compiled instruction.
pub fn decode_instruction(
    ix: &CompiledInstruction,
    account_keys: &[Pubkey],
) -> Result<Option<DecodedInstruction>> {
    let program_id = account_keys
        .get(ix.program_id_index as usize)
        .ok_or_else(|| anyhow!("Program ID index {} out of bounds", ix.program_id_index))?;

    let data = &ix.data;

    // --- Compute Budget Program ---
    if program_id == &solana_sdk::compute_budget::id() {
        if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(micro_lamports)) =
            bincode::deserialize(data)
        {
            return Ok(Some(DecodedInstruction::ComputeBudgetSetUnitPrice {
                micro_lamports,
            }));
        }
    }

    // --- System Program ---
    if program_id == &solana_sdk::system_program::id() {
        if let Ok(system_instruction::SystemInstruction::Transfer { lamports }) =
            bincode::deserialize(data)
        {
            if let (Some(&source), Some(&destination)) = (ix.accounts.get(0), ix.accounts.get(1)) {
                return Ok(Some(DecodedInstruction::SystemTransfer {
                    source: account_keys[source as usize],
                    destination: account_keys[destination as usize],
                    lamports,
                }));
            }
        }
    }

    // --- SPL Token Program (Manual Parsing) ---
    if program_id == &spl_token::id() {
        let Some(discriminator) = data.first() else {
            return Ok(None); // No data, can't be an SPL instruction
        };

        match discriminator {
            // Case for `Transfer`
            3 if data.len() == 9 => {
                let amount = u64::from_le_bytes(data[1..9].try_into()?);
                if let (Some(&source_idx), Some(&dest_idx), Some(&auth_idx)) =
                    (ix.accounts.get(0), ix.accounts.get(1), ix.accounts.get(2))
                {
                    return Ok(Some(DecodedInstruction::SplTokenTransfer {
                        source: account_keys[source_idx as usize],
                        destination: account_keys[dest_idx as usize],
                        authority: account_keys[auth_idx as usize],
                        amount,
                    }));
                }
            }
            // Case for `TransferChecked`
            12 if data.len() == 10 => {
                let amount = u64::from_le_bytes(data[1..9].try_into()?);
                let decimals = data[9];
                if let (Some(&source_idx), Some(&mint_idx), Some(&dest_idx), Some(&auth_idx)) = (
                    ix.accounts.get(0),
                    ix.accounts.get(1),
                    ix.accounts.get(2),
                    ix.accounts.get(3),
                ) {
                    return Ok(Some(DecodedInstruction::SplTokenTransferChecked {
                        source: account_keys[source_idx as usize],
                        destination: account_keys[dest_idx as usize],
                        authority: account_keys[auth_idx as usize],
                        amount,
                        mint: account_keys[mint_idx as usize],
                        decimals: decimals,
                    }));
                }
            }
            // Case for `Burn`
            8 if data.len() == 9 => {
                let amount = u64::from_le_bytes(data[1..9].try_into()?);
                if let (Some(&source_idx), Some(&mint_idx), Some(&auth_idx)) =
                    (ix.accounts.get(0), ix.accounts.get(1), ix.accounts.get(2))
                {
                    return Ok(Some(DecodedInstruction::SplTokenBurn {
                        source_account: account_keys[source_idx as usize],
                        mint: account_keys[mint_idx as usize],
                        authority: account_keys[auth_idx as usize],
                        amount,
                    }));
                }
            }
            // Add other manual parsers here if needed
            _ => {}
        }
    }

    Ok(None)
}
