
#line 1 "policy.rl"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "policy.h"

static Hostent host;
static int addr_p;
static int have_addr;


#line 92 "policy.rl"



#line 19 "policy.c"
static const char _policy_parser_actions[] = {
	0, 1, 3, 1, 4, 1, 6, 1, 
	7, 1, 8, 1, 9, 1, 10, 2, 
	0, 3, 2, 2, 4, 2, 3, 4, 
	2, 5, 1, 3, 8, 0, 3, 4, 
	0, 3, 2, 4, 5, 8, 0, 3, 
	2, 4
};

static const unsigned char _policy_parser_key_offsets[] = {
	0, 0, 4, 9, 11, 12, 19, 21, 
	24, 26, 29, 31, 34, 37, 38, 40, 
	43, 44, 47, 48, 49, 50, 51, 52, 
	53, 55, 57, 62, 67, 73, 74, 75, 
	76, 78, 82, 86, 90, 94, 96, 97, 
	98, 99, 100, 101, 102, 103, 104, 106, 
	109, 111, 114, 116, 119, 122, 125, 126, 
	129, 130, 135, 140, 141, 142, 143, 144, 
	145, 146, 147, 148, 149, 151, 153, 156, 
	158, 161, 163, 166, 169, 170, 172, 176, 
	180, 184, 188, 190, 193, 194, 197, 198, 
	203, 208, 209, 210, 211, 212, 213, 214, 
	215, 216, 217, 218, 221, 223, 225, 227, 
	229, 229, 232, 235
};

static const char _policy_parser_trans_keys[] = {
	97, 108, 48, 57, 32, 45, 46, 48, 
	57, 32, 45, 62, 32, 97, 104, 108, 
	114, 48, 57, 48, 57, 46, 48, 57, 
	48, 57, 46, 48, 57, 48, 57, 58, 
	48, 57, 58, 48, 57, 58, 48, 57, 
	46, 48, 57, 46, 46, 48, 57, 46, 
	110, 121, 97, 115, 104, 32, 123, 32, 
	123, 32, 97, 108, 48, 57, 32, 46, 
	125, 48, 57, 32, 97, 108, 125, 48, 
	57, 110, 121, 58, 48, 57, 32, 125, 
	48, 57, 32, 125, 48, 57, 32, 125, 
	48, 57, 32, 125, 48, 57, 32, 125, 
	111, 99, 97, 108, 104, 111, 115, 116, 
	48, 57, 46, 48, 57, 48, 57, 46, 
	48, 57, 48, 57, 58, 48, 57, 58, 
	48, 57, 46, 48, 57, 46, 46, 48, 
	57, 46, 32, 46, 125, 48, 57, 32, 
	46, 125, 48, 57, 111, 99, 97, 108, 
	104, 111, 115, 116, 114, 32, 123, 48, 
	57, 46, 48, 57, 48, 57, 46, 48, 
	57, 48, 57, 58, 48, 57, 58, 48, 
	57, 58, 48, 57, 32, 45, 48, 57, 
	32, 45, 48, 57, 32, 45, 48, 57, 
	32, 45, 48, 57, 32, 45, 46, 48, 
	57, 46, 46, 48, 57, 46, 32, 45, 
	46, 48, 57, 32, 45, 46, 48, 57, 
	110, 121, 111, 99, 97, 108, 104, 111, 
	115, 116, 46, 48, 57, 48, 57, 48, 
	57, 48, 57, 48, 57, 46, 48, 57, 
	46, 48, 57, 0
};

static const char _policy_parser_single_lengths[] = {
	0, 2, 3, 2, 1, 5, 0, 1, 
	0, 1, 0, 1, 1, 1, 0, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	2, 2, 3, 3, 4, 1, 1, 1, 
	0, 2, 2, 2, 2, 2, 1, 1, 
	1, 1, 1, 1, 1, 1, 0, 1, 
	0, 1, 0, 1, 1, 1, 1, 1, 
	1, 3, 3, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 2, 0, 1, 0, 
	1, 0, 1, 1, 1, 0, 2, 2, 
	2, 2, 2, 1, 1, 1, 1, 3, 
	3, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 0, 0, 0, 0, 
	0, 1, 1, 0
};

static const char _policy_parser_range_lengths[] = {
	0, 1, 1, 0, 0, 1, 1, 1, 
	1, 1, 1, 1, 1, 0, 1, 1, 
	0, 1, 0, 0, 0, 0, 0, 0, 
	0, 0, 1, 1, 1, 0, 0, 0, 
	1, 1, 1, 1, 1, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 1, 1, 
	1, 1, 1, 1, 1, 1, 0, 1, 
	0, 1, 1, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 1, 1, 1, 
	1, 1, 1, 1, 0, 1, 1, 1, 
	1, 1, 0, 1, 0, 1, 0, 1, 
	1, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 1, 1, 1, 1, 1, 
	0, 1, 1, 0
};

static const short _policy_parser_index_offsets[] = {
	0, 0, 4, 9, 12, 14, 21, 23, 
	26, 28, 31, 33, 36, 39, 41, 43, 
	46, 48, 51, 53, 55, 57, 59, 61, 
	63, 66, 69, 74, 79, 85, 87, 89, 
	91, 93, 97, 101, 105, 109, 112, 114, 
	116, 118, 120, 122, 124, 126, 128, 130, 
	133, 135, 138, 140, 143, 146, 149, 151, 
	154, 156, 161, 166, 168, 170, 172, 174, 
	176, 178, 180, 182, 184, 187, 189, 192, 
	194, 197, 199, 202, 205, 207, 209, 213, 
	217, 221, 225, 228, 231, 233, 236, 238, 
	243, 248, 250, 252, 254, 256, 258, 260, 
	262, 264, 266, 268, 271, 273, 275, 277, 
	279, 280, 283, 286
};

static const char _policy_parser_indicies[] = {
	2, 3, 1, 0, 4, 5, 6, 7, 
	0, 8, 9, 0, 10, 0, 10, 12, 
	13, 14, 15, 11, 0, 16, 0, 17, 
	18, 0, 19, 0, 20, 21, 0, 22, 
	0, 24, 23, 0, 24, 25, 0, 24, 
	0, 26, 0, 20, 27, 0, 20, 0, 
	17, 28, 0, 17, 0, 29, 0, 25, 
	0, 30, 0, 31, 0, 32, 0, 33, 
	34, 0, 35, 36, 0, 36, 38, 39, 
	37, 0, 40, 41, 43, 42, 0, 44, 
	38, 39, 43, 37, 0, 45, 0, 46, 
	0, 47, 0, 48, 0, 40, 43, 49, 
	0, 40, 43, 50, 0, 40, 43, 51, 
	0, 40, 43, 52, 0, 40, 43, 0, 
	53, 0, 54, 0, 55, 0, 56, 0, 
	57, 0, 58, 0, 59, 0, 46, 0, 
	60, 0, 61, 62, 0, 63, 0, 64, 
	65, 0, 66, 0, 47, 67, 0, 47, 
	46, 0, 64, 68, 0, 64, 0, 61, 
	69, 0, 61, 0, 40, 41, 43, 70, 
	0, 40, 41, 43, 51, 0, 71, 0, 
	72, 0, 73, 0, 74, 0, 75, 0, 
	76, 0, 77, 0, 25, 0, 78, 0, 
	79, 80, 0, 81, 0, 82, 83, 0, 
	84, 0, 85, 86, 0, 87, 0, 89, 
	88, 0, 89, 90, 0, 89, 0, 91, 
	0, 4, 5, 92, 0, 4, 5, 93, 
	0, 4, 5, 94, 0, 4, 5, 95, 
	0, 4, 5, 0, 85, 96, 0, 85, 
	0, 82, 97, 0, 82, 0, 4, 5, 
	6, 98, 0, 4, 5, 6, 94, 0, 
	99, 0, 90, 0, 100, 0, 101, 0, 
	102, 0, 103, 0, 104, 0, 105, 0, 
	106, 0, 90, 0, 107, 108, 0, 109, 
	0, 110, 0, 111, 0, 112, 0, 0, 
	107, 113, 0, 107, 111, 0, 0, 0
};

static const char _policy_parser_trans_targs[] = {
	0, 2, 89, 91, 3, 4, 69, 87, 
	3, 4, 5, 99, 19, 21, 59, 67, 
	7, 8, 17, 9, 10, 15, 11, 12, 
	14, 13, 100, 16, 18, 20, 22, 23, 
	24, 25, 26, 25, 26, 27, 29, 38, 
	28, 46, 57, 107, 28, 30, 31, 32, 
	33, 34, 35, 36, 37, 39, 40, 41, 
	42, 43, 44, 45, 47, 48, 55, 49, 
	50, 53, 51, 52, 54, 56, 58, 60, 
	61, 62, 63, 64, 65, 66, 68, 25, 
	26, 70, 71, 85, 72, 73, 83, 74, 
	75, 77, 76, 78, 79, 80, 81, 82, 
	84, 86, 88, 90, 92, 93, 94, 95, 
	96, 97, 98, 6, 105, 101, 102, 103, 
	104, 106
};

static const char _policy_parser_trans_actions[] = {
	13, 31, 15, 15, 5, 5, 1, 21, 
	0, 0, 0, 36, 27, 0, 27, 0, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	24, 1, 18, 1, 1, 1, 0, 0, 
	0, 11, 11, 0, 0, 31, 15, 15, 
	7, 1, 21, 7, 0, 1, 1, 24, 
	18, 3, 3, 3, 3, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 21, 1, 
	1, 1, 1, 1, 1, 1, 0, 9, 
	9, 1, 1, 1, 1, 1, 1, 1, 
	1, 24, 1, 18, 3, 3, 3, 3, 
	1, 1, 21, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 21, 3, 3, 3, 
	3, 21
};

static const char _policy_parser_eof_actions[] = {
	0, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 13, 13, 13, 13, 13, 
	13, 13, 13, 7, 7, 7, 7, 7, 
	7, 7, 7, 0
};

static const int policy_parser_start = 1;
static const int policy_parser_first_final = 99;
static const int policy_parser_error = 0;

static const int policy_parser_en_main = 1;


#line 95 "policy.rl"

Policy *ParsePolicy(const char *p) {
  Policy *policy = malloc(sizeof(Policy));

  memset(policy, 0, sizeof(Policy));
  host.addr = NULL;
  
#line 237 "policy.c"
	{
	 policy->cs = policy_parser_start;
	}

#line 102 "policy.rl"

  policy->p = p;
  policy->pe = p + strlen(p);
  policy->eof = policy->pe;

  
#line 249 "policy.c"
	{
	int _klen;
	unsigned int _trans;
	const char *_acts;
	unsigned int _nacts;
	const char *_keys;

	if ( ( policy->p) == ( policy->pe) )
		goto _test_eof;
	if (  policy->cs == 0 )
		goto _out;
_resume:
	_keys = _policy_parser_trans_keys + _policy_parser_key_offsets[ policy->cs];
	_trans = _policy_parser_index_offsets[ policy->cs];

	_klen = _policy_parser_single_lengths[ policy->cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + _klen - 1;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + ((_upper-_lower) >> 1);
			if ( (*( policy->p)) < *_mid )
				_upper = _mid - 1;
			else if ( (*( policy->p)) > *_mid )
				_lower = _mid + 1;
			else {
				_trans += (unsigned int)(_mid - _keys);
				goto _match;
			}
		}
		_keys += _klen;
		_trans += _klen;
	}

	_klen = _policy_parser_range_lengths[ policy->cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + (_klen<<1) - 2;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + (((_upper-_lower) >> 1) & ~1);
			if ( (*( policy->p)) < _mid[0] )
				_upper = _mid - 2;
			else if ( (*( policy->p)) > _mid[1] )
				_lower = _mid + 2;
			else {
				_trans += (unsigned int)((_mid - _keys)>>1);
				goto _match;
			}
		}
		_trans += _klen;
	}

_match:
	_trans = _policy_parser_indicies[_trans];
	 policy->cs = _policy_parser_trans_targs[_trans];

	if ( _policy_parser_trans_actions[_trans] == 0 )
		goto _again;

	_acts = _policy_parser_actions + _policy_parser_trans_actions[_trans];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 )
	{
		switch ( *_acts++ )
		{
	case 0:
#line 18 "policy.rl"
	{
    addr_p = 0;
    host.addr = NULL;
    have_addr = 0;
  }
	break;
	case 1:
#line 24 "policy.rl"
	{
    have_addr = 1;
  }
	break;
	case 2:
#line 28 "policy.rl"
	{
    host.port = 0;
  }
	break;
	case 3:
#line 32 "policy.rl"
	{
    if (host.addr == NULL) {
      host.addr = malloc(16 * sizeof(char));
    }
    host.addr[addr_p] = (*( policy->p));
    addr_p++;
  }
	break;
	case 4:
#line 40 "policy.rl"
	{
    host.port = host.port * 10 + ((*( policy->p)) - '0');
  }
	break;
	case 5:
#line 44 "policy.rl"
	{
    host.addr[addr_p] = '\0';
  }
	break;
	case 6:
#line 48 "policy.rl"
	{
    if (!have_addr) {
      free(host.addr);
      host.addr = NULL;
    }
    policy->listen = host;
    host.addr = NULL;
  }
	break;
	case 7:
#line 57 "policy.rl"
	{
    if (!have_addr) {
      free(host.addr);
      host.addr = NULL;
    }
    policy->nhost++;
    policy->hosts = realloc(policy->hosts, sizeof(Hostent) * policy->nhost);
    policy->hosts[policy->nhost - 1] = host;
    host.addr = NULL;
  }
	break;
	case 8:
#line 68 "policy.rl"
	{
    policy->type = PROXY_RR;
  }
	break;
	case 9:
#line 72 "policy.rl"
	{
    policy->type = PROXY_HASH;
  }
	break;
	case 10:
#line 76 "policy.rl"
	{
    LogFatal("policy syntax error around:\"%s\"\n", ( policy->p));
  }
	break;
#line 407 "policy.c"
		}
	}

_again:
	if (  policy->cs == 0 )
		goto _out;
	if ( ++( policy->p) != ( policy->pe) )
		goto _resume;
	_test_eof: {}
	if ( ( policy->p) == ( policy->eof) )
	{
	const char *__acts = _policy_parser_actions + _policy_parser_eof_actions[ policy->cs];
	unsigned int __nacts = (unsigned int) *__acts++;
	while ( __nacts-- > 0 ) {
		switch ( *__acts++ ) {
	case 7:
#line 57 "policy.rl"
	{
    if (!have_addr) {
      free(host.addr);
      host.addr = NULL;
    }
    policy->nhost++;
    policy->hosts = realloc(policy->hosts, sizeof(Hostent) * policy->nhost);
    policy->hosts[policy->nhost - 1] = host;
    host.addr = NULL;
  }
	break;
	case 10:
#line 76 "policy.rl"
	{
    LogFatal("policy syntax error around:\"%s\"\n", ( policy->p));
  }
	break;
#line 442 "policy.c"
		}
	}
	}

	_out: {}
	}

#line 108 "policy.rl"

  if (policy->cs == 
#line 453 "policy.c"
0
#line 109 "policy.rl"
) {
    free(policy);
    return NULL;
  }

  return policy;
}

void FreePolicy(Policy *policy) {
  int i;
  free(policy->listen.addr);
  for (i = 0; i < policy->nhost; i++) {
    free(policy->hosts[i].addr);
  }
  free(policy->hosts);
  free(policy);
}
