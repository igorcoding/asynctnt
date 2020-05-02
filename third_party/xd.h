#ifndef XD_H__
#define XD_H__

#include <stdint.h>
#include <stdio.h>

#define HEX_SZ (16*10 + 1)
#define CHR_SZ (16*8 + 1)

typedef struct {
	uint8_t row;
	uint8_t hpad;
	uint8_t cpad;
	uint8_t hsp;
	uint8_t csp;
	uint8_t cols;
} xd_conf;

static xd_conf default_xd_conf = { 16,1,0,1,1,4 };

static char * xd_extra(char *data, size_t size, xd_conf *cf)
{
	/* dumps size bytes of *data to stdout. Looks like:
	 * [0000] 75 6E 6B 6E 6F 77 6E 20 30 FF 00 00 00 00 39 00 unknown 0.....9.
	 * src = 16 bytes.
	 * dst = 6       +  16 * 3   +      4*2         +  16       + 1
	 *       prefix    byte+pad    sp between col    visual     newline
	 */
	if (!cf) cf = &default_xd_conf;
	uint8_t row  = cf->row;
	uint8_t hpad = cf->hpad;
	uint8_t cpad = cf->cpad;
	uint8_t hsp  = cf->hsp;
	uint8_t csp  = cf->csp;
	uint8_t sp   = cf->cols;

	uint8_t every = (uint8_t)row / sp;

	char *p = data;
	unsigned char c;
	size_t n;
	// unsigned addr;
	// char bytestr[4] = {0};
	char addrstr[10] = {0};
	char hexstr[ HEX_SZ ] = {0};
	char chrstr[ CHR_SZ ] = {0};
	unsigned hex_sz = row*(2+hpad) + hsp * sp + 1; /* size = bytes<16*2> + 16*<hpad> + col<hsp*sp> */
	unsigned chr_sz = row*(2+cpad) + csp * sp + 1; /* size = bytes<16> + 16*cpad + col<csp*sp> */

	if ( hex_sz > HEX_SZ ) {
		fprintf(stderr,"Parameters too big: estimated hex size will be %u, but have only %u\n", hex_sz, HEX_SZ);
		return NULL;
	}
	if ( chr_sz > CHR_SZ ) {
		fprintf(stderr,"Parameters too big: estimated chr size will be %u, but have only %u\n", chr_sz, CHR_SZ);
		return NULL;
	}

	size_t sv_sz = ( size + row-1 ) * ( (uint8_t)( 6 + 3 + hex_sz + 2 + chr_sz + 1 + row-1 ) / row );
	/*                      ^ reserve for incomplete string             \n      ^ emulation of ceil */
	char *rv = malloc(sv_sz);
	if (!rv) {
		fprintf(stderr,"Can't allocate memory\n");
		return NULL;
	}
	char *rvptr = rv;

	char *curhex = hexstr;
	char *curchr = chrstr;
	for(n=1; n<=size; n++) {
		if (n % row == 1)
			snprintf(addrstr, sizeof(addrstr), "%04x", ( (int)(p-data) ) & 0xffff );

		c = *p;
		if (c < 0x20 || c > 0x7f) {
			c = '.';
		}

		/* store hex str (for left side) */
		snprintf(curhex, 3+hpad, "%02X%-*s", (unsigned char)*p, hpad,""); curhex += 2+hpad;

		/* store char str (for right side) */
		snprintf(curchr, 2+cpad, "%c%-*s", c, cpad, ""); curchr += 1+cpad;

		//warn("n=%d, row=%d, every=%d\n",n,row,every);
		if( n % row == 0 ) {
			/* line completed */
			//printf("[%-4.4s]   %s  %s\n", addrstr, hexstr, chrstr);
			rvptr += snprintf(rvptr, (p-rvptr+sv_sz) ,"[%-4.4s]   %s  %s\n", addrstr, hexstr, chrstr);
			//sv_catpvf(rv,"[%-4.4s]   %-*s %-*s\n", addrstr, hex_sz-1, hexstr, chr_sz-1, chrstr);
			hexstr[0] = 0; curhex = hexstr;
			chrstr[0] = 0; curchr = chrstr;
		} else if( every && ( n % every == 0 ) ) {
			/* half line: add whitespaces */
			snprintf(curhex, 1+hsp, "%-*s", hsp, ""); curhex += hsp;
			snprintf(curchr, 1+csp, "%-*s", csp, ""); curchr += csp;
		}
		p++; /* next byte */
	}

	if (curhex > hexstr) {
		/* print rest of buffer if not empty */
		//printf("[%4.4s]   %s  %s\n", addrstr, hexstr, chrstr);
		rvptr += snprintf(rvptr, (p-rvptr+sv_sz),"[%-4.4s]   %-*s %-*s\n", addrstr, hex_sz-1, hexstr, chr_sz-1, chrstr);
	}
	//warn("String len: %d, sv_sz=%d",SvCUR(rv),sv_sz);
	return rv;
}

static char * xd(char *data, size_t size)
{
    return xd_extra(data, size, NULL);
}

#endif
